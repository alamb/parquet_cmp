# Parquet Implementation Compare Tool

This program is for testing the correctness of changes in the [parquet](https://crates.io/crates/parquet) crate with existing corpus of parquet files.


# Usage:

```shell
./parquet_cmp <directory_with_parquet_files>
```

# Description
This crate reads parquet files into arrow `RecordBatches` using two different parquet implementations and compares the results are equal.




It was initially created for
https://github.com/apache/arrow-rs/pull/1284
