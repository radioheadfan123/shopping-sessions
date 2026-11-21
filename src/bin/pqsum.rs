//! Order-independent digest of an output folder: per table, row count and a sum of row hashes.
//! Two folders hold the same rows (as a multiset) iff the lines match. Usage: pqsum DIR
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::hash::{DefaultHasher, Hash, Hasher};

fn main() {
    let dir = std::path::PathBuf::from(std::env::args().nth(1).expect("usage: pqsum DIR"));
    for t in ["user_daily", "campaign_daily"] {
        let (mut n, mut sum) = (0u64, 0u64);
        for e in std::fs::read_dir(dir.join(t)).unwrap() {
            let r = SerializedFileReader::new(std::fs::File::open(e.unwrap().path()).unwrap()).unwrap();
            for row in r.get_row_iter(None).unwrap() {
                let mut h = DefaultHasher::new();
                row.unwrap().to_string().hash(&mut h);
                sum = sum.wrapping_add(h.finish());
                n += 1;
            }
        }
        println!("{t} rows {n} sum {sum:016x}");
    }
}
