use std::{path::PathBuf, fs::File, io::{BufReader, BufRead, BufWriter, Write}, thread, sync::{Arc, Mutex}};

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
  let args = Args::parse();
  let mut handles = vec![];

  // Create a thread-safe buffered writer to stdout
  // Stdout has significantly better performance when buffered
  let writer = Arc::new(Mutex::new(BufWriter::new(std::io::stdout())));

  let is_first_line = Arc::new(Mutex::new(true));

  for path in args.files {
    let writer = Arc::clone(&writer);
    let is_first_line = Arc::clone(&is_first_line);

    let handle = thread::spawn(move|| {
      let file = File::open(path.as_path()).unwrap();
      let mut reader = BufReader::new(file);

      // This will take the filename and format it like this: ,"filename.ext"\n
      // It can easily be appended to a row in a CSV file
      let filename_csv = format!(",\"{}\"\n", path.file_name().unwrap().to_str().unwrap()).as_bytes().to_owned();
      
      // Use bytes vector to read a line from the file
      // Before, String was used, but there was lots of overhead with utf8 conversions
      // Sending bytes directly to the writer should eliminate that overhead
      let mut buf = vec![];

      // Read the first line regardless of whether it is output
      // This means the CSV header will be skipped for other files
      let mut csv_header = String::new();
      reader.read_line(&mut csv_header).unwrap();

      // Print the csv header if we are on the first line
      let mut is_first = is_first_line.lock().unwrap();
      if *is_first {
        let csv_header_trim = csv_header.trim_end();
        println!("{csv_header_trim},\"filename\"");
        *is_first = false;
      }
      drop(is_first);

      loop {
        buf.clear();
        match reader.read_until(b'\n', &mut buf) {
          Ok(0) => break,
          Ok(mut n) => {
            // Strip both LF and CRLF line endings
            while buf[n-1] == b'\n' || buf[n-1] == b'\r' {
              n -= 1;
            }
            // Write CSV row appended by filename column
            let mut w = writer.lock().unwrap();
            w.write_all(&buf[..n]).unwrap();
            w.write_all(&filename_csv).unwrap();
            drop(w);
          },
          _ => break
        }
      }
    });
    handles.push(handle);
  }

  for handle in handles {
    handle.join().unwrap();
  }
}