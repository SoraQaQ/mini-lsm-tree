// Copyright (c) 2022-2026 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }
        let older_sst_size = snapshot.levels[snapshot.levels.len() - 1].1.len();
        let mut sst_size = 0;
        for i in 0..snapshot.levels.len() - 1 {
            sst_size += snapshot.levels[i].1.len();
        }

        if sst_size as f64 / older_sst_size as f64 * 100.0
            >= self.options.max_size_amplification_percent as f64
        {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        let mut newer_size = snapshot.levels[0].1.len();

        for i in 1..snapshot.levels.len() {
            let ratio = snapshot.levels[i].1.len() as f64 / newer_size as f64;

            if ratio > 1.0 + self.options.size_ratio as f64 / 100.0
                && i >= self.options.min_merge_width
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[..i].to_vec(),
                    bottom_tier_included: false,
                });
            }

            newer_size += snapshot.levels[i].1.len();
        }

        let count = self
            .options
            .max_merge_width
            .unwrap_or(snapshot.levels.len())
            .min(snapshot.levels.len());

        Some(TieredCompactionTask {
            tiers: snapshot.levels[..count].to_vec(),
            bottom_tier_included: count == snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let first_tier_id = task.tiers[0].0;

        let start = snapshot
            .levels
            .iter()
            .position(|(tier_id, _)| *tier_id == first_tier_id)
            .expect("compaction input tier missing");

        let end = start + task.tiers.len();

        assert_eq!(&snapshot.levels[start..end], task.tiers.as_slice());

        let mut new_snapshot = snapshot.clone();

        let replacement = output.first().map(|&tier_id| (tier_id, output.to_vec()));

        new_snapshot.levels.splice(start..end, replacement);

        let obsolete_ids = snapshot.levels[start..end]
            .iter()
            .flat_map(|(_, sst_ids)| sst_ids.iter().copied())
            .collect();

        (new_snapshot, obsolete_ids)
    }
}
