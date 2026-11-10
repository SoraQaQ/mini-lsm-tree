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

use std::sync::Arc;

use anyhow::{Ok, Result};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }

        let first_sstable = sstables[0].clone();

        let current = SsTableIterator::create_and_seek_to_first(first_sstable)?;

        Ok(Self {
            current: Some(current),
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let idx = sstables.partition_point(|sst| sst.last_key().as_key_slice() < key);

        if idx == sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }

        let sstable = sstables[idx].clone();

        let current = SsTableIterator::create_and_seek_to_key(sstable, key)?;

        Ok(Self {
            current: Some(current),
            next_sst_idx: idx + 1,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        self.current.as_ref().expect("iterator is invalid").key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().expect("iterator is invalid").value()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|iter| iter.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        let Some(currnet) = self.current.as_mut() else {
            return Ok(());
        };

        currnet.next()?;

        if !self.is_valid() {
            self.current = None;

            if self.next_sst_idx < self.sstables.len() {
                let table = self.sstables[self.next_sst_idx].clone();
                self.current = Some(SsTableIterator::create_and_seek_to_first(table)?);

                self.next_sst_idx += 1;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
