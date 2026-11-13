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
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let first_sstable = &snapshot.sstables[&sst_ids[0]];
        let mut first_key = first_sstable.first_key();
        let mut last_key = first_sstable.last_key();

        for sst_id in &sst_ids[1..] {
            let sstable = &snapshot.sstables[sst_id];
            if sstable.first_key() < first_key {
                first_key = sstable.first_key();
            }

            if sstable.last_key() > last_key {
                last_key = sstable.last_key();
            }
        }

        let sst_ids = &snapshot.levels[in_level - 1].1;

        let mut overlapping_sst_ids = Vec::new();
        for sst_id in sst_ids {
            let sstable = &snapshot.sstables[sst_id];
            if sstable.first_key() <= last_key && sstable.last_key() >= first_key {
                overlapping_sst_ids.push(*sst_id);
            }
        }

        overlapping_sst_ids
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let level_sizes: Vec<u64> = snapshot
            .levels
            .iter()
            .map(|(_, sst_ids)| {
                sst_ids
                    .iter()
                    .map(|id| snapshot.sstables[id].table_size())
                    .sum()
            })
            .collect();

        let base_bytes = self.options.base_level_size_mb as u64 * 1024 * 1024;
        let mut target_sizes = vec![0u64; level_sizes.len()];
        let bottom = level_sizes.len() - 1;

        target_sizes[bottom] = level_sizes[bottom].max(base_bytes);

        for i in (0..bottom).rev() {
            if target_sizes[i + 1] > base_bytes {
                target_sizes[i] = target_sizes[i + 1] / self.options.level_size_multiplier as u64;
            }
        }

        let base_level = target_sizes
            .iter()
            .position(|&size| size > 0)
            .expect("bottom level must have a positive target")
            + 1;

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let mut ratio: Option<(usize, f64)> = None;

        if target_sizes[0] > 0 {
            ratio = Some((0, level_sizes[0] as f64 / target_sizes[0] as f64))
        }

        for i in 1..target_sizes.len() - 1 {
            if target_sizes[i] > 0 {
                let new_ratio = (i, level_sizes[i] as f64 / target_sizes[i] as f64);
                match ratio {
                    Some(current) => {
                        if new_ratio.1 > current.1 {
                            ratio = Some(new_ratio);
                        }
                    }
                    None => ratio = Some(new_ratio),
                }
            }
        }

        if let Some(ratio) = ratio
            && ratio.1 > 1.0
        {
            return Some(LeveledCompactionTask {
                upper_level: Some(ratio.0 + 1usize),
                upper_level_sst_ids: vec![
                    snapshot.levels[ratio.0].1.iter().copied().min().unwrap(),
                ],
                lower_level: ratio.0 + 2,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &[snapshot.levels[ratio.0].1.iter().copied().min().unwrap()],
                    ratio.0 + 2,
                ),
                is_lower_level_bottom_level: ratio.0 + 2 == self.options.max_levels,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        match task.upper_level {
            Some(upper_level) => {
                snapshot.levels[upper_level - 1]
                    .1
                    .retain(|x| !task.upper_level_sst_ids.contains(x));
                snapshot.levels[task.lower_level - 1]
                    .1
                    .retain(|x| !task.lower_level_sst_ids.contains(x));
            }
            None => {
                snapshot
                    .l0_sstables
                    .retain(|x| !task.upper_level_sst_ids.contains(x));
                snapshot.levels[task.lower_level - 1]
                    .1
                    .retain(|x| !task.lower_level_sst_ids.contains(x));
            }
        }

        snapshot.levels[task.lower_level - 1]
            .1
            .extend_from_slice(output);

        let obsolete_ids = task
            .upper_level_sst_ids
            .iter()
            .chain(task.lower_level_sst_ids.iter())
            .copied()
            .collect();

        (snapshot, obsolete_ids)
    }
}
