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

use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::{BufMut, Bytes};

use super::{BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
    table::{FileObject, bloom::Bloom},
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        let builder = BlockBuilder::new(block_size);
        Self {
            builder,
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));
        if self.builder.add(key, value) {
            if self.first_key.is_empty() {
                self.first_key = key.raw_ref().to_vec();
            }
            self.last_key = key.raw_ref().to_vec();
            return;
        }

        assert!(
            !self.builder.is_empty(),
            "empty block fit in an empty block"
        );

        let old_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = old_builder.build();

        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.first_key)),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
        });

        self.data.extend_from_slice(&block.encode());

        assert!(
            self.builder.add(key, value),
            "entry cannot fit in an empty block"
        );

        self.first_key = key.raw_ref().to_vec();
        self.last_key = key.raw_ref().to_vec();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        #[allow(unused_mut)] mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if self.meta.is_empty() && self.builder.is_empty() {
            anyhow::bail!("cannot build an empty SST");
        }

        if !self.builder.is_empty() {
            let block = self.builder.build();

            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.first_key)),
                last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
            });

            self.data.extend_from_slice(&block.encode());
        }
        let block_meta_offset = self.data.len();

        let encode_meta_offset = u32::try_from(block_meta_offset)?;

        BlockMeta::encode_block_meta(&self.meta, &mut self.data);

        self.data.put_u32(encode_meta_offset);

        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);

        let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);

        let bloom_offset = u32::try_from(self.data.len())?;

        bloom.encode(&mut self.data);

        self.data.put_u32(bloom_offset);

        let first_key = self.meta.first().unwrap().first_key.clone();
        let last_key = self.meta.last().unwrap().last_key.clone();

        let file = FileObject::create(path.as_ref(), self.data)?;

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
