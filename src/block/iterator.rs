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

use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let first_key = if block.offsets.is_empty() {
            KeyVec::new()
        } else {
            let offset = block.offsets[0] as usize;
            let data = &block.data;

            let key_len = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;
            let key_start = offset + 4;

            KeyVec::from_vec(data[key_start..key_start + key_len].to_vec())
        };

        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        let idx = self.idx;
        self.seek_to(idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut left = 0;
        let mut right = self.block.offsets.len();
        while left < right {
            let mid = left + (right - left) / 2;
            self.seek_to(mid);
            match self.key.as_key_slice().cmp(&key) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Equal => right = mid,
                std::cmp::Ordering::Greater => right = mid,
            }
        }

        self.seek_to(left);
    }

    fn seek_to(&mut self, idx: usize) {
        self.idx = idx;
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        let offset = self.block.offsets[idx] as usize;

        let data = &self.block.data;

        let overlap_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;

        let rest_len = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;

        let key_start = offset + SIZEOF_U16 + SIZEOF_U16;
        let key_end = key_start + rest_len;

        let mut raw_key = Vec::new();

        raw_key.extend_from_slice(&self.first_key.raw_ref()[..overlap_len]);
        raw_key.extend_from_slice(&data[key_start..key_end]);

        let key = KeyVec::from_vec(raw_key);

        self.key = key;

        let value_len = u16::from_be_bytes([data[key_end], data[key_end + 1]]) as usize;
        let value_start = key_end + SIZEOF_U16;
        let value_end = value_start + value_len;

        self.value_range = (value_start, value_end);
    }
}
