// Copyright 2019-2022 ChainSafe Systems
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::{
    ActorMigration, ActorMigrationInput, MigrationError, MigrationOutput, MigrationResult,
};

use actor_interface::{
    actorv2::multisig::State as V2State,
    actorv3::{
        multisig::{State as V3State, TxnID as TxnIdV3},
        MULTISIG_ACTOR_CODE_ID,
    },
};

use cid::{Cid, Code::Blake2b256};
use fil_types::HAMT_BIT_WIDTH;
use ipld_blockstore::BlockStore;
use std::sync::Arc;

use super::migrate_hamt_raw;

pub struct MultisigMigrator(Cid);

pub fn multisig_migrator_v3<BS: BlockStore + Send + Sync>(
    cid: Cid,
) -> Arc<dyn ActorMigration<BS> + Send + Sync> {
    Arc::new(MultisigMigrator(cid))
}

impl<BS: BlockStore + Send + Sync> ActorMigration<BS> for MultisigMigrator {
    fn migrate_state(
        &self,
        store: Arc<BS>,
        input: ActorMigrationInput,
    ) -> MigrationResult<MigrationOutput> {
        let in_state: V2State = store
            .get(&input.head)
            .map_err(|e| MigrationError::BlockStoreRead(e.to_string()))?
            .ok_or_else(|| {
                MigrationError::BlockStoreRead(
                    "Multisig actor: could not read v2 state".to_string(),
                )
            })?;

        let pending_txs = migrate_hamt_raw(store.as_ref(), &in_state.pending_txs, HAMT_BIT_WIDTH)?;

        let out_state = V3State {
            signers: in_state.signers,
            num_approvals_threshold: in_state.num_approvals_threshold,
            next_tx_id: TxnIdV3(in_state.next_tx_id.0),
            initial_balance: in_state.initial_balance,
            start_epoch: in_state.start_epoch,
            unlock_duration: in_state.unlock_duration,
            pending_txs,
        };

        let new_head = store
            .put(&out_state, Blake2b256)
            .map_err(|e| MigrationError::BlockStoreWrite(e.to_string()))?;

        Ok(MigrationOutput {
            new_code_cid: *MULTISIG_ACTOR_CODE_ID,
            new_head,
        })
    }
}
