// Copyright 2019-2022 ChainSafe Systems
// SPDX-License-Identifier: Apache-2.0, MIT

use std::sync::Arc;

use crate::{
    ActorMigration, ActorMigrationInput, MigrationError, MigrationOutput, MigrationResult,
};
use actor_interface::{
    actorv2::init::State as Init2State,
    actorv3::{init::State as Init3State, INIT_ACTOR_CODE_ID},
};

use cid::{Cid, Code::Blake2b256};
use fil_types::HAMT_BIT_WIDTH;
use ipld_blockstore::BlockStore;

use super::migrate_hamt_raw;

pub struct InitMigrator(Cid);

pub fn init_migrator_v3<BS: BlockStore + Send + Sync>(
    cid: Cid,
) -> Arc<dyn ActorMigration<BS> + Send + Sync> {
    Arc::new(InitMigrator(cid))
}

impl<BS: BlockStore + Send + Sync> ActorMigration<BS> for InitMigrator {
    fn migrate_state(
        &self,
        store: std::sync::Arc<BS>,
        input: ActorMigrationInput,
    ) -> MigrationResult<MigrationOutput> {
        let in_state: Init2State = store
            .get(&input.head)
            .map_err(|e| MigrationError::BlockStoreRead(e.to_string()))?
            .ok_or_else(|| {
                MigrationError::BlockStoreRead("Init actor: could not read v2 state".to_string())
            })?;

        let address_map = migrate_hamt_raw(store.as_ref(), &in_state.address_map, HAMT_BIT_WIDTH)
            .map_err(|e| MigrationError::BlockStoreWrite(e.to_string()))?;

        let out_state = Init3State {
            address_map,
            next_id: in_state.next_id,
            network_name: in_state.network_name,
        };

        let new_head = store
            .put(&out_state, Blake2b256)
            .map_err(|e| MigrationError::BlockStoreWrite(e.to_string()))?;

        Ok(MigrationOutput {
            new_code_cid: *INIT_ACTOR_CODE_ID,
            new_head,
        })
    }
}
