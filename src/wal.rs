// REMOVE THIS LINE after fully implementing this functionality
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

use anyhow::{Ok, Result};
use bytes::{BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::KeySlice;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    /// Week 2 Day 6: create a new write-ahead log.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(path)?;

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// Week 2 Day 6: recover entries from a write-ahead log.
    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)?;

        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut pos = 0;
        while pos < buf.len() {
            anyhow::ensure!(buf.len() - pos >= 2, "incomplete WAL key length");
            let key_len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
            pos += 2;
            anyhow::ensure!(buf.len() - pos >= key_len, "incomplete WAL key value");
            let key = &buf[pos..pos + key_len];
            pos += key_len;
            anyhow::ensure!(buf.len() - pos >= 2, "incomplete WAL value length");
            let value_len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
            pos += 2;
            anyhow::ensure!(buf.len() - pos >= value_len, "incomplete WAL key value");
            let value = &buf[pos..pos + value_len];
            pos += value_len;
            skiplist.insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// Week 2 Day 6: append a key-value pair to the write-ahead log.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key_len = u16::try_from(key.len())?;
        let value_len = u16::try_from(value.len())?;
        let mut buf = Vec::new();
        buf.put_u16(key_len);
        buf.put(key);
        buf.put_u16(value_len);
        buf.put(value);
        let mut file = self.file.lock();
        file.write_all(&buf)?;
        Ok(())
    }

    /// Week 3 Day 5: append a batch of key-value pairs.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    /// Week 2 Day 6: synchronize the write-ahead log to storage.
    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;

        Ok(())
    }
}
