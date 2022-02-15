# Parquet Implementation Compare Tool

This program is for testing the correctness of changes in the [parquet](https://crates.io/crates/parquet) crate with existing corpus of parquet files.

This crate reads parquet files into arrow `RecordBatches` using two different parquet implementations and compares the results are equal. It is used to verify proposed changes to the parquet crate.

It was initially created to verify https://github.com/apache/arrow-rs/pull/1284

# Usage:

```shell
./parquet_cmp <directory_with_parquet_files>
```

# Example output:

```shell
$ cargo run --release -- --path ~/Documents/prod_dbs/
...
Both readers had same problem reading "010f0bd7-080f-4bbd-bcbf-a5c1048ef93a.parquet"; skipping file.
...
file "6f0333f2-c50c-463d-a09a-01396bb73504.parquet" compared successfully
...
107 files read with different readers compared successfully
```
