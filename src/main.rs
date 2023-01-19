use std::{path::PathBuf, fs::File, io::{BufReader, BufRead}};

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
    
    let mut print_header = true;
    for path in args.files {
        let file_name = path.file_name().unwrap().to_str().unwrap();

        let file = File::open(path.as_path()).unwrap();
        let buf_reader = BufReader::new(file);
        
        let mut first_line = true;
        for line in buf_reader.lines() {
            if first_line {
                first_line = false;
                if !print_header {
                    continue;
                }
                print_header = false;
                println!("{},\"filename\"", line.unwrap())
            }
            else {
                println!("{},\"{}\"", line.unwrap(), file_name);
            }
        }
    }
}
