// Copyright 2019-2022 ChainSafe Systems
// SPDX-License-Identifier: Apache-2.0, MIT

use std::sync::Arc;

use cid::{Cid, Code::Blake2b256};
use ipld_blockstore::BlockStore;

use crate::{
    ActorMigration, ActorMigrationInput, MigrationError, MigrationOutput, MigrationResult,
};

use actor_interface::{
    actorv2::paych::State as StateV2,
    actorv3::{paych::State as StateV3, PAYCH_ACTOR_CODE_ID},
};

use actor_interface::actorv3::paych::LANE_STATES_AMT_BITWIDTH;

use super::migrate_amt_raw;

pub struct PaychMigrator(Cid);

pub fn paych_migrator_v3<BS: BlockStore + Send + Sync>(
    cid: Cid,
) -> Arc<dyn ActorMigration<BS> + Send + Sync> {
    Arc::new(PaychMigrator(cid))
}

impl<BS: BlockStore + Send + Sync> ActorMigration<BS> for PaychMigrator {
    fn migrate_state(
        &self,
        store: Arc<BS>,
        input: ActorMigrationInput,
    ) -> MigrationResult<MigrationOutput> {
        let in_state: StateV2 = store
            .get(&input.head)
            .map_err(|e| MigrationError::BlockStoreRead(e.to_string()))?
            .ok_or_else(|| {
                MigrationError::BlockStoreRead("Paych actor: could not read v2 state".to_string())
            })?;

        let lane_states = migrate_amt_raw(
            store.as_ref(),
            &in_state.lane_states,
            LANE_STATES_AMT_BITWIDTH,
        )?;

        let out_state = StateV3 {
            from: in_state.from,
            to: in_state.to,
            to_send: in_state.to_send,
            settling_at: in_state.settling_at,
            min_settle_height: in_state.min_settle_height,
            lane_states,
        };

        let new_head = store
            .put(&out_state, Blake2b256)
            .map_err(|e| MigrationError::BlockStoreWrite(e.to_string()))?;

        Ok(MigrationOutput {
            new_code_cid: *PAYCH_ACTOR_CODE_ID,
            new_head,
        })
    }
}
