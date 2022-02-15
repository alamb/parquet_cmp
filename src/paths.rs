use std::{collections::VecDeque, path::PathBuf};

#[derive(Debug)]
/// Recursively and incrementally walks a directory structure. Is
/// likely to get confused if the directories change during iteration
pub struct RecursiveDirectoryIterator {
    worklist: VecDeque<PathBuf>,
}

impl RecursiveDirectoryIterator {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let mut worklist = VecDeque::new();
        worklist.push_back(path.into());
        Self { worklist }
    }
}

impl Iterator for RecursiveDirectoryIterator {
    type Item = PathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(path) = self.worklist.pop_front() {
            let metadata = path.metadata().expect("reading metadata");

            if metadata.is_file() {
                return Some(path);
            } else if metadata.is_dir() {
                // read entries
                let mut new_entries: Vec<PathBuf> = std::fs::read_dir(path)
                    .expect("reading directory")
                    .map(|entry| entry.expect("error reading directory entry").path())
                    .collect();

                new_entries.sort_unstable();
                for entry in new_entries {
                    self.worklist.push_back(entry)
                }
            }
        }

        // Nothing left in worklist
        None
    }
}

/// Recursively find all files that look like they could contain parquet files

pub struct ParquetIter {
    inner: RecursiveDirectoryIterator,
}

impl ParquetIter {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let inner = RecursiveDirectoryIterator::new(path);
        Self { inner }
    }
}

impl Iterator for ParquetIter {
    type Item = PathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let p = self.inner.next()?;

            // skip anything without extension
            let extension = if let Some(extension) = p.extension() {
                extension.to_string_lossy()
            } else {
                continue;
            };

            if extension == "parquet" {
                return Some(p);
            }
        }
    }
}
