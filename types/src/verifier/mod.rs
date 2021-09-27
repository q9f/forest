// Copyright 2020 ChainSafe Systems
// SPDX-License-Identifier: Apache-2.0, MIT

mod mock;

pub use self::mock::*;

use crate::{
    AggregateSealVerifyProofAndInfos, PoStProof, Randomness, RegisteredPoStProof, SealVerifyInfo,
    SectorInfo,
};
use address::Address;
use commcid::{cid_to_data_commitment_v1, cid_to_replica_commitment_v1};
use encoding::bytes_32;
use filecoin_proofs_api::{self as proofs, ProverId, SectorId};
use filecoin_proofs_api::{
    post, seal::verify_aggregate_seal_commit_proofs, seal::verify_seal as proofs_verify_seal,
    PublicReplicaInfo,
};
use proofs::seal;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::error::Error as StdError;

/// Full verification implementation. This will verify all proofs through `rust-fil-proofs`
/// using locally fetched parameters.
pub enum FullVerifier {}

impl ProofVerifier for FullVerifier {}

/// Functionality for verification of seal, winning PoSt and window PoSt proofs.
/// Proof verification will be full validation by default.
pub trait ProofVerifier {
    /// Verify seal proof for sectors. This proof verifies that a sector was sealed by the miner.
    fn verify_seal(vi: &SealVerifyInfo) -> Result<(), Box<dyn StdError>> {
        let commr = cid_to_replica_commitment_v1(&vi.sealed_cid)?;
        let commd = cid_to_data_commitment_v1(&vi.unsealed_cid)?;
        let prover_id = prover_id_from_u64(vi.sector_id.miner);

        if !proofs_verify_seal(
            vi.registered_proof.try_into()?,
            commr,
            commd,
            prover_id,
            SectorId::from(vi.sector_id.number),
            bytes_32(&vi.randomness.0),
            bytes_32(&vi.interactive_randomness.0),
            &vi.proof,
        )? {
            Err("Invalid Seal proof".into())
        } else {
            Ok(())
        }
    }

    fn verify_aggregate_seals(
        aggregate: &AggregateSealVerifyProofAndInfos,
    ) -> Result<(), Box<dyn StdError>> {
        if aggregate.infos.is_empty() {
            return Err("no seal verify infos".into());
        }
        let spt: proofs::RegisteredSealProof = aggregate.seal_proof.try_into()?;
        let prover_id = prover_id_from_u64(aggregate.miner);
        struct AggregationInputs {
            // replica
            commr: [u8; 32],
            // data
            commd: [u8; 32],
            sector_id: SectorId,
            ticket: [u8; 32],
            seed: [u8; 32],
        }
        let inputs: Vec<AggregationInputs> = aggregate
            .infos
            .iter()
            .map(|info| {
                let commr = cid_to_replica_commitment_v1(&info.sealed_cid)?;
                let commd = cid_to_data_commitment_v1(&info.unsealed_cid)?;
                Ok(AggregationInputs {
                    commr,
                    commd,
                    ticket: bytes_32(&info.randomness.0),
                    seed: bytes_32(&info.interactive_randomness.0),
                    sector_id: SectorId::from(info.sector_number),
                })
            })
            .collect::<Result<Vec<_>, &'static str>>()?;

        let inp: Vec<Vec<_>> = inputs
            .par_iter()
            .map(|input| {
                seal::get_seal_inputs(
                    spt,
                    input.commr,
                    input.commd,
                    prover_id,
                    input.sector_id,
                    input.ticket,
                    input.seed,
                )
            })
            .try_reduce(Vec::new, |mut acc, current| {
                acc.extend(current);
                Ok(acc)
            })?;
        let commrs: Vec<[u8; 32]> = inputs.iter().map(|input| input.commr).collect();
        let seeds: Vec<[u8; 32]> = inputs.iter().map(|input| input.seed).collect();
        if !verify_aggregate_seal_commit_proofs(
            spt,
            aggregate.aggregate_proof.try_into()?,
            aggregate.proof.clone(),
            &commrs,
            &seeds,
            inp,
        )? {
            Err("Invalid Aggregate Seal proof".into())
        } else {
            Ok(())
        }
    }
    /// Verifies winning proof of spacetime. These proofs are generated by the miners that are
    /// elected to mine a new block to verify a sector. A failed winning proof leads to a miner
    /// being slashed.
    fn verify_winning_post(
        Randomness(mut randomness): Randomness,
        proofs: &[PoStProof],
        challenge_sectors: &[SectorInfo],
        prover: u64,
    ) -> Result<(), Box<dyn StdError>> {
        // Necessary to be valid bls12 381 element.
        randomness[31] &= 0x3f;

        // Convert sector info into public replica
        let replicas = to_fil_public_replica_infos(&challenge_sectors, ProofType::Winning)?;

        // Convert PoSt proofs into proofs-api format
        let proof_bytes = proofs.iter().fold(Vec::new(), |mut proof, p| {
            proof.extend_from_slice(&p.proof_bytes);
            proof
        });

        // Generate prover bytes from ID
        let prover_id = prover_id_from_u64(prover);

        // Verify Proof
        if !post::verify_winning_post(&bytes_32(&randomness), &proof_bytes, &replicas, prover_id)? {
            Err("Winning post was invalid".into())
        } else {
            Ok(())
        }
    }

    /// Verifies window proof of spacetime. These proofs are generated regularly to audit
    /// commitments made by storage miners. An invalid Window PoSt leads to a miner's block being
    /// invalidated and miss the opportunity to receive block rewards.
    fn verify_window_post(
        Randomness(mut randomness): Randomness,
        proofs: &[PoStProof],
        challenge_sectors: &[SectorInfo],
        prover: u64,
    ) -> Result<(), Box<dyn StdError>> {
        // Necessary to be valid bls12 381 element.
        randomness[31] &= 0x3f;

        // Convert sector info into public replica
        let replicas = to_fil_public_replica_infos(&challenge_sectors, ProofType::Window)?;

        // Convert PoSt proofs into proofs-api format
        let proofs: Vec<(proofs::RegisteredPoStProof, _)> = proofs
            .iter()
            .map(|p| Ok((p.post_proof.try_into()?, p.proof_bytes.as_ref())))
            .collect::<Result<_, String>>()?;

        // Generate prover bytes from ID
        let prover_id = prover_id_from_u64(prover);

        // Verify Proof
        if !post::verify_window_post(&bytes_32(&randomness), &proofs, &replicas, prover_id)? {
            Err("Window post was invalid".into())
        } else {
            Ok(())
        }
    }

    /// Generates sector challenge indexes for use in winning PoSt verification.
    fn generate_winning_post_sector_challenge(
        proof: RegisteredPoStProof,
        prover_id: u64,
        Randomness(mut randomness): Randomness,
        eligible_sector_count: u64,
    ) -> Result<Vec<u64>, Box<dyn StdError>> {
        // Necessary to be valid bls12 381 element.
        randomness[31] &= 0x3f;

        Ok(post::generate_winning_post_sector_challenge(
            proof.try_into()?,
            &bytes_32(&randomness),
            eligible_sector_count,
            prover_id_from_u64(prover_id),
        )?)
    }
}

/// PoSt proof variants.
enum ProofType {
    Winning,
    Window,
}

fn prover_id_from_u64(id: u64) -> ProverId {
    let mut prover_id = ProverId::default();
    let prover_bytes = Address::new_id(id).payload().to_raw_bytes();
    prover_id[..prover_bytes.len()].copy_from_slice(&prover_bytes);
    prover_id
}

fn to_fil_public_replica_infos(
    src: &[SectorInfo],
    typ: ProofType,
) -> Result<BTreeMap<SectorId, PublicReplicaInfo>, String> {
    let replicas = src
        .iter()
        .map::<Result<(SectorId, PublicReplicaInfo), String>, _>(|sector_info: &SectorInfo| {
            let commr = cid_to_replica_commitment_v1(&sector_info.sealed_cid)?;
            let proof = match typ {
                ProofType::Winning => sector_info.proof.registered_winning_post_proof()?,
                ProofType::Window => sector_info.proof.registered_window_post_proof()?,
            };
            let replica = PublicReplicaInfo::new(proof.try_into()?, commr);
            Ok((SectorId::from(sector_info.sector_number), replica))
        })
        .collect::<Result<BTreeMap<SectorId, PublicReplicaInfo>, _>>()?;
    Ok(replicas)
}
