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

use bytes::BufMut;

use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn establish_size(&self) -> usize {
        SIZEOF_U16 + self.offsets.len() * SIZEOF_U16 + self.data.len()
    }

    fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
        let mut prefix = 0;
        for (left, right) in a.iter().zip(b) {
            if left != right {
                break;
            }
            prefix += 1;
        }
        prefix
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        let Ok(key_len) = u16::try_from(key.len()) else {
            return false;
        };

        let Ok(value_len) = u16::try_from(value.len()) else {
            return false;
        };

        let Ok(offset) = u16::try_from(self.data.len()) else {
            return false;
        };

        // add offset
        let overlap = if self.is_empty() {
            0
        } else {
            BlockBuilder::common_prefix_len(self.first_key.as_key_slice().raw_ref(), key.raw_ref())
        };

        let rest = &key.raw_ref()[overlap..];

        let entry_size = rest
            .len()
            .saturating_add(value.len())
            .saturating_add(SIZEOF_U16 * 4);

        let block_is_full = self.establish_size() + entry_size > self.block_size;
        let offset_is_full = self.data.len() > usize::from(u16::MAX);
        let count_is_full = self.offsets.len() >= usize::from(u16::MAX);

        if (block_is_full || offset_is_full || count_is_full) && !self.is_empty() {
            return false;
        }

        if self.is_empty() {
            self.first_key = key.to_key_vec();
        }

        self.offsets.push(offset);

        // add key
        self.data.put_u16(overlap as u16);
        self.data.put_u16(rest.len() as u16);

        self.data.put(rest);

        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
