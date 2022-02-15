mod paths;


use std::{io::stdout, path::PathBuf};

use clap::Parser;

/// Command line program for comparing parquet reader implementations
///
/// # Example read .parquet files in /path/to/files
///
/// parquet_cmp  /path/to/dumps
///
/// # Reference
///
/// [logs]: https://github.com/grpc/proposal/blob/master/A16-binary-logging.md
/// [protobuf]: https://github.com/grpc/grpc-proto/blob/master/grpc/binlog/v1/binarylog.proto
#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct ParquetCmp {
    #[clap(long, parse(from_os_str))]
    /// Search path for parquet files
    path: PathBuf,
}


fn main() {
    let args = ParquetCmp::parse();

    for file in paths::ParquetIter::new(&args.path) {
        print!("Comparing file {:?}", file);

        // TODO actual comparison

        println!("Done");
    }

}


fn main() {
    println!("Hello, world!");
}
