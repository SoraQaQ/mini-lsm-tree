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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}
pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_with_iter<I>(
        &self,
        iter: &mut I,
        compact_to_bottom: bool,
    ) -> Result<Vec<Arc<SsTable>>>
    where
        I: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    {
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut sstable_result = Vec::new();
        let mut has_entries = false;

        while iter.is_valid() {
            if !iter.value().is_empty() || !compact_to_bottom {
                builder.add(iter.key(), iter.value());
                has_entries = true;
                if builder.estimated_size() >= self.options.target_sst_size {
                    let id = self.next_sst_id();
                    let sstable =
                        builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
                    builder = SsTableBuilder::new(self.options.block_size);
                    sstable_result.push(Arc::new(sstable));
                    has_entries = false;
                }
            }
            iter.next()?;
        }

        if has_entries {
            let id = self.next_sst_id();
            let sstable =
                builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;

            sstable_result.push(Arc::new(sstable));
        }

        Ok(sstable_result)
    }

    fn builder_l0_iter(
        &self,
        snapshot: &LsmStorageState,
        l0_sst_ids: &[usize],
    ) -> Result<MergeIterator<SsTableIterator>> {
        let mut l0_iter = Vec::with_capacity(l0_sst_ids.len());
        for sst_id in l0_sst_ids {
            let sstable = snapshot.sstables[sst_id].clone();
            let iter = SsTableIterator::create_and_seek_to_first(sstable)?;
            l0_iter.push(Box::new(iter));
        }

        let merge_upper_iter = MergeIterator::create(l0_iter);
        Ok(merge_upper_iter)
    }

    fn builder_sstcontact_iter(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
    ) -> Result<SstConcatIterator> {
        let mut sstables = Vec::with_capacity(sst_ids.len());
        for sst_id in sst_ids {
            let sstable = snapshot.sstables[sst_id].clone();
            sstables.push(sstable);
        }

        let concat_lower_iter = SstConcatIterator::create_and_seek_to_first(sstables)?;
        Ok(concat_lower_iter)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::Leveled(leveled_compaction_task) => {
                let snapshot = {
                    let guard = self.state.read();
                    Arc::clone(&guard)
                };

                match leveled_compaction_task.upper_level {
                    None => {
                        let merge_upper_iter = self.builder_l0_iter(
                            &snapshot,
                            &leveled_compaction_task.upper_level_sst_ids,
                        )?;

                        let concat_lower_iter = self.builder_sstcontact_iter(
                            &snapshot,
                            &leveled_compaction_task.lower_level_sst_ids,
                        )?;

                        let mut iter =
                            TwoMergeIterator::create(merge_upper_iter, concat_lower_iter)?;

                        self.compact_with_iter(
                            &mut iter,
                            leveled_compaction_task.is_lower_level_bottom_level,
                        )
                    }
                    Some(upper_level) => {
                        let concat_upper_iter = self.builder_sstcontact_iter(
                            &snapshot,
                            &leveled_compaction_task.upper_level_sst_ids,
                        )?;

                        let concat_lower_iter = self.builder_sstcontact_iter(
                            &snapshot,
                            &leveled_compaction_task.lower_level_sst_ids,
                        )?;
                        let mut iter =
                            TwoMergeIterator::create(concat_upper_iter, concat_lower_iter)?;

                        self.compact_with_iter(
                            &mut iter,
                            leveled_compaction_task.is_lower_level_bottom_level,
                        )
                    }
                }
            }
            CompactionTask::Tiered(tiered_compaction_task) => {
                let snapshot = {
                    let guard = self.state.read();
                    Arc::clone(&guard)
                };

                let tiers = &tiered_compaction_task.tiers;

                let mut merge_iters = Vec::new();
                for (_, sst_ids) in tiers {
                    let iter = self.builder_sstcontact_iter(&snapshot, sst_ids)?;
                    merge_iters.push(Box::new(iter));
                }

                let mut iter = MergeIterator::create(merge_iters);

                self.compact_with_iter(&mut iter, tiered_compaction_task.bottom_tier_included)
            }
            CompactionTask::Simple(simple_leveled_compaction_task) => {
                let snapshot = {
                    let guard = self.state.read();
                    Arc::clone(&guard)
                };

                match simple_leveled_compaction_task.upper_level {
                    Some(level) => {
                        let upper_iter = self.builder_sstcontact_iter(
                            &snapshot,
                            &simple_leveled_compaction_task.upper_level_sst_ids,
                        )?;

                        let lower_iter = self.builder_sstcontact_iter(
                            &snapshot,
                            &simple_leveled_compaction_task.lower_level_sst_ids,
                        )?;

                        let mut iter = TwoMergeIterator::create(upper_iter, lower_iter)?;

                        self.compact_with_iter(
                            &mut iter,
                            simple_leveled_compaction_task.is_lower_level_bottom_level,
                        )
                    }
                    None => {
                        let merge_upper_iter = self.builder_l0_iter(
                            &snapshot,
                            &simple_leveled_compaction_task.upper_level_sst_ids,
                        )?;

                        let lower_iter = self.builder_sstcontact_iter(
                            &snapshot,
                            &simple_leveled_compaction_task.lower_level_sst_ids,
                        )?;

                        let mut iter = TwoMergeIterator::create(merge_upper_iter, lower_iter)?;

                        self.compact_with_iter(
                            &mut iter,
                            simple_leveled_compaction_task.is_lower_level_bottom_level,
                        )
                    }
                }
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let snapshot = {
                    let guard = self.state.read();
                    Arc::clone(&guard)
                };
                let mut iters = Vec::with_capacity(l0_sstables.len() + l1_sstables.len());
                for sst_id in l0_sstables.iter().chain(l1_sstables.iter()) {
                    let sstable = snapshot.sstables[sst_id].clone();
                    let iter = SsTableIterator::create_and_seek_to_first(sstable)?;
                    iters.push(Box::new(iter));
                }

                let mut iter = MergeIterator::create(iters);
                self.compact_with_iter(&mut iter, true)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let task = {
            let guard = self.state.read();
            CompactionTask::ForceFullCompaction {
                l0_sstables: guard.l0_sstables.clone(),
                l1_sstables: guard.levels[0].1.clone(),
            }
        };

        let new_ssts = self.compact(&task)?;

        self.sync_dir()?;

        let CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        } = &task
        else {
            unreachable!();
        };

        {
            let _state_lock = self.state_lock.lock();
            let recorded_task = CompactionTask::ForceFullCompaction {
                l0_sstables: l0_sstables.clone(),
                l1_sstables: l1_sstables.clone(),
            };
            let output = new_ssts.iter().map(|sst| sst.sst_id()).collect();
            if let Some(ref manifset) = self.manifest {
                manifset.add_record(
                    &_state_lock,
                    crate::manifest::ManifestRecord::Compaction(recorded_task, output),
                )?;
            }
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            snapshot.l0_sstables.retain(|id| !l0_sstables.contains(id));

            for id in l0_sstables.iter().chain(l1_sstables.iter()) {
                snapshot.sstables.remove(id);
            }

            snapshot.levels[0].1 = new_ssts.iter().map(|sst| sst.sst_id()).collect();

            for sst in &new_ssts {
                snapshot.sstables.insert(sst.sst_id(), sst.clone());
            }

            *guard = Arc::new(snapshot);
        }

        for id in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*id))?;
        }
        self.sync_dir()?;

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let task = {
            let guard = self.state.read();
            self.compaction_controller.generate_compaction_task(&guard)
        };

        if let Some(task) = task {
            let new_sst = self.compact(&task)?;
            let output: Vec<usize> = new_sst.iter().map(|sst| sst.sst_id()).collect();
            self.sync_dir()?;

            let obsolete_ids = {
                let _state_lock = self.state_lock.lock();
                let mut guard = self.state.write();
                let (mut snapshot, obsolete_ids) = self
                    .compaction_controller
                    .apply_compaction_result(guard.as_ref(), &task, &output, false);

                for sstable in &new_sst {
                    snapshot.sstables.insert(sstable.sst_id(), sstable.clone());
                }

                if let CompactionTask::Leveled(task) = &task {
                    snapshot.levels[task.lower_level - 1]
                        .1
                        .sort_by_key(|sst_id| snapshot.sstables[sst_id].first_key());
                }

                if let Some(ref manifest) = self.manifest {
                    manifest.add_record(
                        &_state_lock,
                        crate::manifest::ManifestRecord::Compaction(task, output),
                    )?;
                }
                for id in &obsolete_ids {
                    snapshot.sstables.remove(id);
                }

                *guard = Arc::new(snapshot);

                obsolete_ids
            };

            for id in obsolete_ids {
                std::fs::remove_file(self.path_of_sst(id))?;
            }
            self.sync_dir()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        if snapshot.imm_memtables.len() >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
