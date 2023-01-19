use std::{path::PathBuf, fs::File, io::{BufReader, BufRead}, thread, sync::{Arc, Mutex}};

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "CSV Combiner")]
#[command(author = "Dane Rieber")]
#[command(version = "1.0")]
#[command(about = "Combines rows from multiple CSV files into one CSV file, adding a column specifying the original file for each row")]
#[command(long_about = None)]
struct Args {
    /// Specify input CSV files
    files: Vec<PathBuf>
}

fn main() {
  const CAP: usize = 16 * 1024;

  let args = Args::parse();
  let queue = Arc::new(Mutex::new(String::with_capacity(CAP)));
  let added_header_mtx = Arc::new(Mutex::new(false));
  let mut handlers = vec![];

  for path in args.files {
    let queue = Arc::clone(&queue);
    let added_header_mtx = Arc::clone(&added_header_mtx);
    let handle = thread::spawn(move|| {
      let file = File::open(path.as_path()).expect("Error opening file");
      let filename_csv = format!(",\"{}\"\n", path.file_name().unwrap().to_str().unwrap());
      let mut reader = BufReader::new(file);

      let mut header = String::new();
      reader.read_line(&mut header).unwrap();
      let header_trimmed = header.trim_end();
      let trim_n_chars = header.len() - header_trimmed.len();

      let mut added_header = added_header_mtx.lock().unwrap();
      if !*added_header {
        print!("{header_trimmed}{filename_csv}");
        *added_header = true;
      }

      loop {
        let mut q = queue.lock().unwrap();
        match reader.read_line(&mut q) {
          Ok(0) => {
            break;
          },
          Ok(n) => {
            let len = q.len();
            if n <= trim_n_chars {
              break;
            }
            q.truncate(len - trim_n_chars);
            q.push_str(&filename_csv);
            if len > CAP {
              print!("{q}");
              q.clear();
            }
          },
          _ => {}
        }
      }
    });
    handlers.push(handle);
  }

  for handle in handlers {
    handle.join().unwrap();
  }

  print!("{}", queue.lock().unwrap());
}