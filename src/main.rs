mod paths;

use std::path::PathBuf;

use arrow::{
    array::as_dictionary_array,
    array::Array,
    compute::eq_dyn,
    datatypes::{DataType, Field, Int32Type},
    record_batch::RecordBatch,
};
use clap::Parser;

/// Command line program for comparing parquet reader implementations
#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct ParquetCmp {
    #[clap(long, parse(from_os_str))]
    /// Search path for parquet files
    path: PathBuf,
}

#[tokio::main]
async fn main() {
    let args = ParquetCmp::parse();

    let tasks: Vec<_> = paths::ParquetIter::new(&args.path)
        .map(|file| tokio::task::spawn(file_compare(file)))
        .collect();

    let num_tasks = tasks.len();
    for task in tasks {
        task.await.unwrap();
    }

    println!(
        "{} files read with different readers compared successfully",
        num_tasks
    );
}

async fn file_compare(file: PathBuf) {
    match (
        parquet9::read_to_serialized_record_batches(&file),
        parquetnext::read_to_serialized_record_batches(&file),
    ) {
        (Ok(p9_data), Ok(pnext_data)) => {
            compare(p9_data, pnext_data);
        }
        (Err(p9_err), Err(pn_err)) if p9_err.to_string() == pn_err.to_string() => {
            //println!("  both readers had problems reading: {}, {}", p9_err, pn_err);
            println!(
                "Both readers had same problem reading {:?}; skipping file. ",
                file
            );
            return;
        }
        _ => panic!("one reader got success, one got failure"),
    };
    println!("file {:?} compared successfully", file);
}

fn compare(p9_data: Vec<u8>, pnext_data: Vec<u8>) {
    //println!("  read {} bytes from parquet9, {} from parquet next", p9_data.len(), pnext_data.len());

    // Deserialzie the data into two batches and then compare them
    let p9_batches = to_batches(p9_data);
    let pnext_batches = to_batches(pnext_data);

    //println!("  read {} batches from parquet9, {} from parquet next", p9_batches.len(), pnext_batches.len());

    assert_eq!(p9_batches.len(), pnext_batches.len());
    if p9_batches.is_empty() {
        return;
    }

    assert_eq!(p9_batches[0].schema(), pnext_batches[0].schema());

    // now compare the batches
    for (p9_batch, pnext_batch) in p9_batches.into_iter().zip(pnext_batches.into_iter()) {
        //println!("    comparing batch [{}]", batch_idx);
        for ((p9_col, pnext_col), field) in p9_batch
            .columns()
            .iter()
            .zip(pnext_batch.columns().iter())
            .zip(p9_batch.schema().fields().iter())
        {
            //println!("      comparing column {}", field.name());

            match field.data_type() {
                DataType::Dictionary(key_type, value_type)
                    if matches!(key_type.as_ref(), &DataType::Int32)
                        && matches!(value_type.as_ref(), &DataType::Utf8) =>
                {
                    let p9_col = as_dictionary_array::<Int32Type>(p9_col);
                    let pnext_col = as_dictionary_array::<Int32Type>(pnext_col);
                    cmp_array(field, p9_col.keys(), pnext_col.keys());
                    cmp_array(field, p9_col.values(), pnext_col.values());
                }
                _ => {
                    // Any non null values should be the same
                    cmp_array(field, p9_col, pnext_col);
                }
            };
        }
    }
}

fn cmp_array(field: &Field, p9_col: &dyn Array, pnext_col: &dyn Array) {
    assert_eq!(p9_col.len(), pnext_col.len());

    // Should have same null / non null
    for i in 0..p9_col.len() {
        assert_eq!(p9_col.is_valid(i), pnext_col.is_valid(i))
    }

    // Any non null values should be the same
    let cmp_result = eq_dyn(p9_col, pnext_col).unwrap();
    assert!(
        cmp_result.iter().all(|v| v.unwrap_or(true)),
        "comparison not true for column {}: {:?}",
        field.name(),
        cmp_result
    );
}

fn to_batches(data: Vec<u8>) -> Vec<RecordBatch> {
    let reader = arrow::ipc::reader::StreamReader::try_new(&*data).unwrap();

    reader.map(|r| r.unwrap()).collect()
}
