use std::path::Path;
use std::fmt::Display;
use arrow::record_batch::RecordBatchReader;
use parquet::file::reader::SerializedFileReader;
use parquet::arrow::{ParquetFileArrowReader, ArrowReader};
use std::sync::Arc;
use std::fs::File;


#[derive(Debug, Clone)]
pub struct Error {
    msg: String
}

impl std::error::Error for Error {
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error: {}", self.msg)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error { msg: format!("io error: {}", e) }
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Error { msg: format!("parquet error: {}", e) }
    }
}

impl From<arrow::error::ArrowError> for Error {
    fn from(e: arrow::error::ArrowError) -> Self {
        Error { msg: format!("arrow error: {}", e) }
    }
}


/// Returns Arrow IPC format (
///
/// Decode using https://docs.rs/arrow/9.0.2/arrow/ipc/reader/index.html
pub fn read_to_serialized_record_batches(path: &Path) -> Result<Vec<u8>, Error> {
    println!("  Reading {:?} in parquet9", path);
    let file = File::open(path)?;
    let file_reader = SerializedFileReader::new(file)?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    //println!("Converted arrow schema is: {}", arrow_reader.get_schema()?);

    let reader = arrow_reader.get_record_reader(2048)?;
    let mut ipc_writer = arrow::ipc::writer::StreamWriter::try_new(Vec::<u8>::new(), &reader.schema())?;

    for batch in reader {
        let batch = batch?;

        ipc_writer.write(&batch)?;
    }

    ipc_writer.finish()?;

    Ok(ipc_writer.into_inner()?)
}
