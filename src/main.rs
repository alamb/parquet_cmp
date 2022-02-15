mod paths;


use std::path::PathBuf;

use clap::Parser;

/// Command line program for comparing parquet reader implementations
///
/// # Example read .parquet files in /path/to/files
///
///
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

        let (parquet9_data, parquetnext_data) = match (parquet9::read_to_serialized_record_batches(&file),
                                                       parquetnext::read_to_serialized_record_batches(&file)) {
            (Ok(p9), Ok(pn)) => (p9, pn),
            (Err(p9_err), Err(pn_err)) if p9_err.to_string() == pn_err.to_string() => {
                //println!("  both readers had problems reading: {}, {}", p9_err, pn_err);
                println!("  Both readers had same problem reading; skipping file. ");
                continue;
            }
            _ => todo!()
        };

        println!("  read {} bytes from parquet9, {} from parquet next", parquet9_data.len(), parquetnext_data.len());


        // TODO actual comparison

        println!("Done");
    }

}
