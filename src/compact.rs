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
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::Leveled(leveled_compaction_task) => todo!(),
            CompactionTask::Tiered(tiered_compaction_task) => {
                let snapshot = {
                    let guard = self.state.read();
                    Arc::clone(&guard)
                };

                let mut sstable_result = Vec::new();

                let tiers = &tiered_compaction_task.tiers;

                let mut merge_iters = Vec::new();
                for (_, sst_ids) in tiers {
                    let mut sstables = Vec::new();
                    for sst_id in sst_ids {
                        let sstable = snapshot.sstables[sst_id].clone();
                        sstables.push(sstable);
                    }

                    let iter = SstConcatIterator::create_and_seek_to_first(sstables)?;
                    merge_iters.push(Box::new(iter));
                }

                let mut iter = MergeIterator::create(merge_iters);

                let mut has_entries = false;

                let mut builder = SsTableBuilder::new(self.options.block_size);

                while iter.is_valid() {
                    if !iter.value().is_empty() || !tiered_compaction_task.bottom_tier_included {
                        builder.add(iter.key(), iter.value());
                        has_entries = true;

                        if builder.estimated_size() >= self.options.target_sst_size {
                            let id = self.next_sst_id();
                            let sstable = builder.build(
                                id,
                                Some(self.block_cache.clone()),
                                self.path_of_sst(id),
                            )?;
                            builder = SsTableBuilder::new(self.options.block_size);
                            has_entries = false;
                            sstable_result.push(Arc::new(sstable));
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
            CompactionTask::Simple(simple_leveled_compaction_task) => {
                let snapshot = {
                    let guard = self.state.read();
                    Arc::clone(&guard)
                };

                let mut sstable_result = Vec::new();

                match simple_leveled_compaction_task.upper_level {
                    Some(level) => {
                        let mut upper_sstables = Vec::with_capacity(
                            simple_leveled_compaction_task.upper_level_sst_ids.len(),
                        );
                        for sst_id in &simple_leveled_compaction_task.upper_level_sst_ids {
                            let sstable = snapshot.sstables[sst_id].clone();
                            upper_sstables.push(sstable);
                        }

                        let upper_iter =
                            SstConcatIterator::create_and_seek_to_first(upper_sstables)?;

                        let mut lower_sstables = Vec::with_capacity(
                            simple_leveled_compaction_task.lower_level_sst_ids.len(),
                        );

                        for sst_id in &simple_leveled_compaction_task.lower_level_sst_ids {
                            let sstable = snapshot.sstables[sst_id].clone();
                            lower_sstables.push(sstable);
                        }

                        let lower_iter =
                            SstConcatIterator::create_and_seek_to_first(lower_sstables)?;

                        let mut two_merger_iter = TwoMergeIterator::create(upper_iter, lower_iter)?;

                        let mut builder = SsTableBuilder::new(self.options.block_size);

                        let mut has_entries = false;

                        while two_merger_iter.is_valid() {
                            if !two_merger_iter.value().is_empty()
                                || !task.compact_to_bottom_level()
                            {
                                builder.add(two_merger_iter.key(), two_merger_iter.value());
                                has_entries = true;
                                if builder.estimated_size() >= self.options.target_sst_size {
                                    let id = self.next_sst_id();
                                    let sstable = builder.build(
                                        id,
                                        Some(self.block_cache.clone()),
                                        self.path_of_sst(id),
                                    )?;
                                    sstable_result.push(Arc::new(sstable));
                                    builder = SsTableBuilder::new(self.options.block_size);
                                    has_entries = false;
                                }
                            }
                            two_merger_iter.next()?;
                        }

                        if has_entries {
                            let id = self.next_sst_id();
                            let sstable = builder.build(
                                id,
                                Some(self.block_cache.clone()),
                                self.path_of_sst(id),
                            )?;
                            sstable_result.push(Arc::new(sstable));
                        }
                    }
                    None => {
                        let mut l0_iters = Vec::with_capacity(
                            simple_leveled_compaction_task.upper_level_sst_ids.len(),
                        );

                        for sst_id in &simple_leveled_compaction_task.upper_level_sst_ids {
                            let sstable = snapshot.sstables[sst_id].clone();
                            let iter = SsTableIterator::create_and_seek_to_first(sstable)?;
                            l0_iters.push(Box::new(iter));
                        }

                        let upper_iter = MergeIterator::create(l0_iters);

                        let mut lower_sstables = Vec::with_capacity(
                            simple_leveled_compaction_task.lower_level_sst_ids.len(),
                        );

                        for sst_id in &simple_leveled_compaction_task.lower_level_sst_ids {
                            let sstable = snapshot.sstables[sst_id].clone();
                            lower_sstables.push(sstable);
                        }

                        let lower_iter =
                            SstConcatIterator::create_and_seek_to_first(lower_sstables)?;

                        let mut two_merger_iter = TwoMergeIterator::create(upper_iter, lower_iter)?;

                        let mut builder = SsTableBuilder::new(self.options.block_size);

                        let mut has_entries = false;

                        while two_merger_iter.is_valid() {
                            if !two_merger_iter.value().is_empty()
                                || !task.compact_to_bottom_level()
                            {
                                builder.add(two_merger_iter.key(), two_merger_iter.value());
                                has_entries = true;
                                if builder.estimated_size() >= self.options.target_sst_size {
                                    let id = self.next_sst_id();
                                    let sstable = builder.build(
                                        id,
                                        Some(self.block_cache.clone()),
                                        self.path_of_sst(id),
                                    )?;
                                    sstable_result.push(Arc::new(sstable));
                                    builder = SsTableBuilder::new(self.options.block_size);
                                    has_entries = false;
                                }
                            }
                            two_merger_iter.next()?;
                        }

                        if has_entries {
                            let id = self.next_sst_id();
                            let sstable = builder.build(
                                id,
                                Some(self.block_cache.clone()),
                                self.path_of_sst(id),
                            )?;
                            sstable_result.push(Arc::new(sstable));
                        }
                    }
                }

                Ok(sstable_result)
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
                let mut builder = SsTableBuilder::new(self.options.block_size);

                let mut sstable_result = Vec::new();

                let mut has_entries = false;

                while iter.is_valid() {
                    if !iter.value().is_empty() {
                        builder.add(iter.key(), iter.value());
                        has_entries = true;
                        if builder.estimated_size() >= self.options.target_sst_size {
                            let id = self.next_sst_id();
                            let sstable = builder.build(
                                id,
                                Some(self.block_cache.clone()),
                                self.path_of_sst(id),
                            )?;
                            sstable_result.push(Arc::new(sstable));
                            builder = SsTableBuilder::new(self.options.block_size);
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

        let CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        } = &task
        else {
            unreachable!();
        };

        {
            let _state_lock = self.state_lock.lock();
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

            let obsolete_ids = {
                let _state_lock = self.state_lock.lock();
                let mut guard = self.state.write();
                let (mut snapshot, obsolete_ids) = self
                    .compaction_controller
                    .apply_compaction_result(guard.as_ref(), &task, &output, false);

                for sstable in &new_sst {
                    snapshot.sstables.insert(sstable.sst_id(), sstable.clone());
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
