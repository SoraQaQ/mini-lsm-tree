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

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    choose_a: bool,
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let choose_a = if !a.is_valid() {
            false
        } else if !b.is_valid() {
            true
        } else {
            a.key() <= b.key()
        };

        Ok(Self { a, b, choose_a })
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.choose_a {
            return self.a.key();
        }
        self.b.key()
    }

    fn value(&self) -> &[u8] {
        if self.choose_a {
            return self.a.value();
        }
        self.b.value()
    }

    fn is_valid(&self) -> bool {
        if self.choose_a {
            return self.a.is_valid();
        }
        self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        let same_key = self.a.is_valid() && self.b.is_valid() && self.a.key() == self.b.key();

        if same_key {
            self.a.next()?;
            self.b.next()?;
        } else if self.choose_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }

        self.choose_a = if !self.a.is_valid() {
            false
        } else if !self.b.is_valid() {
            true
        } else {
            self.a.key() <= self.b.key()
        };

        Ok(())
    }
}
