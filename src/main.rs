mod tests;

use std::{path::PathBuf, fs::File, io::{BufReader, BufRead, Write, self}, thread, sync::{Arc, Mutex, MutexGuard}};

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "CSV Combiner")]
#[command(author = "Dane Rieber")]
#[command(version = "1.0")]
#[command(about = "Combines rows from multiple CSV files into one CSV file, adding a column specifying the original file for each row")]
#[command(long_about = None)]
struct Args {
    /// Specify input CSV files
    #[clap(required = true)]
    files: Vec<PathBuf>,

    /// Limit the maximum number of threads
    #[arg(short, long, default_value_t = 64)]
    threads: usize
}

// On my local machine, I found that very big buffers work much faster
// For example, if we use BUFFER_CAPACITY 16kb and THREAD_BUFFER_CAPACITY 1kb,
// the program runs nearly twice as slow
pub const BUFFER_CAPACITY: usize = 256 * 1024;
pub const THREAD_BUFFER_CAPACITY: usize = 16 * 1024;

fn main() {
  let args = Args::parse();
  let mut files = args.files.iter().cloned();

  let mut handles = vec![];

  // A Vec<u8> buffer that will be written to stdout and cleared when it reaches capacity
  let stdout_buf = Arc::new(Mutex::new(Vec::with_capacity(BUFFER_CAPACITY)));

  let is_first_line = Arc::new(Mutex::new(true));
  loop {
    // Spawn new threads until we reach the user-specified limit
    if handles.len() < args.threads {
      if let Some(path) = files.next() {
        let stdout_buf = Arc::clone(&stdout_buf);
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
          let mut buf = Vec::with_capacity(THREAD_BUFFER_CAPACITY);
    
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
    
          // Closure to take care of writing to output buffer and flushing output buffer to stdout if it reaches capacity
          let write_buf = |out_buf: &mut MutexGuard<Vec<u8>>, buf: &mut Vec<u8>| {
            out_buf.write_all(&buf).unwrap();
            if out_buf.len() >= BUFFER_CAPACITY {
              let mut stdout = io::stdout().lock();
              stdout.write_all(&out_buf).unwrap();
              out_buf.clear();
            }
            buf.clear();
          };
    
          loop {
            match reader.read_until(b'\n', &mut buf) {
              Ok(0) => {
                // Write any left over data that may still be in the thread buffer
                let mut out_buf = stdout_buf.lock().unwrap();
                write_buf(&mut out_buf, &mut buf);
                break;
              },
              Ok(_) => {
                // Strip both LF and CRLF line endings
                while buf[buf.len()-1] == b'\n' || buf[buf.len()-1] == b'\r' {
                  buf.pop();
                }
                // Append filename column to buffer
                buf.write_all(&filename_csv).unwrap();
    
                // Try to acquire lock on output buffer. If we can't, no worries, just keep reading data
                if let Ok(mut out_buf) = stdout_buf.try_lock() {
                  write_buf(&mut out_buf, &mut buf);
                // HOWEVER, if our thread-local buffer fills up, we must acquire the lock and write our data
                } else if buf.len() >= THREAD_BUFFER_CAPACITY {
                  let mut out_buf = stdout_buf.lock().unwrap();
                  write_buf(&mut out_buf, &mut buf);
                }
              },
              _ => break
            }
          }
        });

        handles.push(handle);

      } else {
        break;
      }
    } else {
      // If we have the maximum number of threads, wait for one to finish
      handles.pop().unwrap().join().unwrap();
    }
  }

  // Finish up all reminaing threads
  for handle in handles {
    handle.join().unwrap();
  }

  // Write any left over data still in the output buffer
  let out_buf = stdout_buf.lock().unwrap();
  let mut stdout = io::stdout().lock();
  stdout.write_all(&out_buf).unwrap();
}