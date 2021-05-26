use std::marker::PhantomData;
use std::sync::Arc;

use jsonrpc_derive::rpc;
use node_rpc::IoHandler;
use sc_client_api::blockchain::{HeaderBackend, HeaderMetadata};
use sc_client_api::{backend, Backend, BlockBackend, StorageProvider};
use serde::{Deserialize, Serialize};
use sp_api::{ApiExt, Core, ProvideRuntimeApi};
use sp_runtime::traits::Header;
use sp_runtime::{generic::BlockId, traits::Block as BlockT};

/// Storage key.
pub type StorageKey = Vec<u8>;

/// Storage value.
pub type StorageValue = Vec<u8>;

/// In memory array of storage values.
pub type StorageCollection = Vec<(StorageKey, Option<StorageValue>)>;

/// In memory arrays of storage values for multiple child tries.
pub type ChildStorageCollection = Vec<(StorageKey, StorageCollection)>;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct StorageChanges {
    // TODO: encode changes into some kind of binary format
    /// A value of `None` means that it was deleted.
    pub main_storage_changes: StorageCollection,
    /// All changes to the child storages.
    pub child_storage_changes: ChildStorageCollection,
}

/// Response for the `state_storageChanges` RPC.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetStorageChangesResponse(Vec<StorageChanges>);

/// State RPC errors.
#[derive(Debug, derive_more::Display, derive_more::From)]
pub enum Error {
    /// Provided block range couldn't be resolved to a list of blocks.
    #[display(fmt = "Cannot resolve a block range ['{:?}' ... '{:?}].", from, to)]
    InvalidBlockRange {
        /// Beginning of the block range.
        from: String,
        /// End of the block range.
        to: String,
    },
    /// Error occurred when processing some block
    #[display(fmt = "Error occurred when processing the block {:?}.", self.0)]
    InvalidBlock(String),
}

impl Error {
    fn invalid_block<Block: BlockT>(id: BlockId<Block>) -> Self {
        Self::InvalidBlock(format!("{}", id))
    }
}

/// Base code for all errors.
const BASE_ERROR: i64 = 10000;

impl From<Error> for jsonrpc_core::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::InvalidBlockRange { .. } => jsonrpc_core::Error {
                code: jsonrpc_core::ErrorCode::ServerError(BASE_ERROR + 1),
                message: format!("{:?}", e),
                data: None,
            },
            Error::InvalidBlock(_) => jsonrpc_core::Error {
                code: jsonrpc_core::ErrorCode::ServerError(BASE_ERROR + 2),
                message: format!("{:?}", e),
                data: None,
            },
        }
    }
}

#[rpc]
pub trait NodeRpcExtApi<BlockHash> {
    #[rpc(name = "pha_getStorageChanges")]
    fn get_storage_changes(
        &self,
        from: BlockHash,
        to: BlockHash,
    ) -> Result<GetStorageChangesResponse, Error>;
}

/// Stuffs for custom RPC
struct NodeRpcExt<BE, Block: BlockT, Client> {
    client: Arc<Client>,
    backend: Arc<BE>,
    _phantom: PhantomData<Block>,
}

impl<BE, Block: BlockT, Client> NodeRpcExt<BE, Block, Client> {
    fn new(client: Arc<Client>, backend: Arc<BE>) -> Self {
        Self {
            client,
            backend,
            _phantom: Default::default(),
        }
    }
}

impl<BE: 'static, Block: BlockT, Client: 'static> NodeRpcExtApi<Block::Hash>
    for NodeRpcExt<BE, Block, Client>
where
    BE: Backend<Block>,
    Client: StorageProvider<Block, BE>
        + HeaderBackend<Block>
        + BlockBackend<Block>
        + HeaderMetadata<Block, Error = sp_blockchain::Error>
        + ProvideRuntimeApi<Block>,
    Client::Api:
        sp_api::Metadata<Block> + ApiExt<Block, StateBackend = backend::StateBackendFor<BE, Block>>,
    Block: BlockT + 'static,
{
    fn get_storage_changes(
        &self,
        from: Block::Hash,
        to: Block::Hash,
    ) -> Result<GetStorageChangesResponse, Error> {
        // TODO: This operation is heavy and will block the async executor,
        //  consider return a Future and run the task in another thread.
        get_storage_changes(self.client.as_ref(), self.backend.as_ref(), from, to)
    }
}

fn get_storage_changes<Client, BE, Block>(
    client: &Client,
    backend: &BE,
    from: Block::Hash,
    to: Block::Hash,
) -> Result<GetStorageChangesResponse, Error>
where
    BE: Backend<Block>,
    Client: StorageProvider<Block, BE>
        + HeaderBackend<Block>
        + BlockBackend<Block>
        + HeaderMetadata<Block, Error = sp_blockchain::Error>
        + ProvideRuntimeApi<Block>,
    Block: BlockT + 'static,
    Client::Api:
        sp_api::Metadata<Block> + ApiExt<Block, StateBackend = backend::StateBackendFor<BE, Block>>,
{
    fn header<Client: HeaderBackend<Block>, Block: BlockT>(
        client: &Client,
        id: BlockId<Block>,
    ) -> Result<Block::Header, Error> {
        client
            .header(id)
            .map_err(|_| Error::invalid_block(id))?
            .ok_or_else(|| Error::invalid_block(id))
    }

    let n_from = *header(client, BlockId::Hash(from))?.number();
    let n_to = *header(client, BlockId::Hash(to))?.number();
    if n_from >= n_to {
        return Err(Error::InvalidBlockRange {
            from: format!("{}({})", from, n_from),
            to: format!("{}({})", to, n_to),
        });
    }

    let api = client.runtime_api();
    let mut changes = vec![];
    let mut this_block = to;

    loop {
        let id = BlockId::Hash(this_block);
        let mut header = header(client, id)?;
        let extrinsics = client
            .block_body(&id)
            .map_err(|_| Error::invalid_block(id))?
            .ok_or_else(|| Error::invalid_block(id))?;
        let parent_hash = *header.parent_hash();
        let parent_id = BlockId::Hash(parent_hash);

        // Remove all `Seal`s as they are added by the consensus engines after building the block.
        // On import they are normally removed by the consensus engine.
        header.digest_mut().logs.retain(|d| d.as_seal().is_none());

        let block = Block::new(header, extrinsics);
        api.execute_block(&parent_id, block)
            .map_err(|_| Error::invalid_block(id))?;

        let state = backend
            .state_at(parent_id)
            .map_err(|_| Error::invalid_block(parent_id))?;

        let storage_changes = api
            .into_storage_changes(&state, None, parent_hash)
            .map_err(|_| Error::invalid_block(parent_id))?;

        changes.push(StorageChanges {
            main_storage_changes: storage_changes.main_storage_changes,
            child_storage_changes: storage_changes.child_storage_changes,
        });
        if parent_hash == from {
            break;
        } else {
            this_block = parent_hash;
        }
    }
    Ok(GetStorageChangesResponse(changes))
}

pub fn extend_rpc<Client, BE, Block>(io: &mut IoHandler, client: Arc<Client>, backend: Arc<BE>)
where
    BE: Backend<Block> + 'static,
    Client: StorageProvider<Block, BE>
        + HeaderBackend<Block>
        + BlockBackend<Block>
        + HeaderMetadata<Block, Error = sp_blockchain::Error>
        + ProvideRuntimeApi<Block>
        + 'static,
    Block: BlockT + 'static,
    Client::Api:
        sp_api::Metadata<Block> + ApiExt<Block, StateBackend = backend::StateBackendFor<BE, Block>>,
{
    io.extend_with(NodeRpcExtApi::to_delegate(NodeRpcExt::new(client, backend)));
}
