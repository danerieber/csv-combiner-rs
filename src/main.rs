use std::{path::PathBuf, fs::File, io::{BufReader, BufRead, Read}, thread, sync::{Arc, Mutex}};

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
  let queue = Arc::new(Mutex::new(String::with_capacity(16 * 1024)));
  let mut handles = vec![];

  for path in args.files {
    let queue = Arc::clone(&queue);
    let handle = thread::spawn(move|| {
      let file = File::open(path.as_path()).expect("Error opening file");
      let filename_csv = format!(",\"{}\"\n", path.file_name().unwrap().to_str().unwrap());
      let reader = BufReader::new(file);
      for line in reader.lines() {
        if let Ok(text) = line {
          let mut q = queue.lock().unwrap();
          if q.len() + text.len() + filename_csv.len() > q.capacity() {
            print!("{q}");
            q.clear();
          }
          q.push_str(&text);
          q.push_str(&filename_csv);
        }
      }
    });
    handles.push(handle);
  }

  for handle in handles {
    handle.join().unwrap();
  }

  print!("{}", queue.lock().unwrap());
}

// fn main() {
//     let args = Args::parse();
    
//     // Build vec of readers for threads to use
//     let mut readers = vec![];
//     for path in args.files {
//       let file = File::open(path.as_path()).expect("Error opening file");
//       let file_name = format!(",\"{}\"\n", path.file_name().unwrap().to_str().unwrap());
//       let reader = BufReader::new(file);
//       readers.push((reader, file_name));
//     }

//     // Extract CSV header from one of the files (just use the first one)
//     let mut csv_header = String::new();
//     readers[0].0.read_line(&mut csv_header).expect("Error reading CSV header from first file");
//     // Remove trailing newline and add "filename" column header
//     csv_header.pop();
//     csv_header.push_str(",\"filename\"\n");
//     // Print CSV header only once
//     print!("{}", csv_header);

//     // Use a synchronous, bounded channel to queue lines for output
//     let (tx, rx) = sync_channel(100);

//     for (reader, file_name) in readers {
//       let tx = tx.clone();
//       // Spawn a new thread that sends each line of each file as a message over the channel
//       thread::spawn(move|| {
//         for line in reader.lines().skip(1) {
//           if let Ok(text) = line {
//             tx.send(format!("{text}{file_name}")).unwrap();
//           }
//         }
//       });
//     }

//     drop(tx); // Drop last sender to stop rx waiting for message

//     // Create large string that we use as a buffer
//     const CAP: usize = 16 * 1024;
//     let mut q = String::with_capacity(CAP);

//     while let Ok(msg) = rx.recv() {
//       // if we cannot add the current msg to the queue, print to stdout and clear queue
//       if q.len() + msg.len() > CAP {
//         print!("{q}");
//         q.clear();
//       }
//       q.push_str(&msg);
//     }

//     // print leftover data in queue
//     if q.len() > 0 {
//       print!("{q}");
//     }
// }