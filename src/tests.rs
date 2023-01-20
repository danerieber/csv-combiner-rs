#[cfg(test)]
mod tests {
  use std::{fs::File, io::{BufReader, BufRead}};

use assert_cmd::Command;
  #[test]
  fn exits_when_no_args_provided() {
    let mut cmd = Command::cargo_bin("csv-combiner-rs").unwrap();
    cmd.assert().failure();
  }

  #[test]
  fn exits_when_no_files_provided() {
    let mut cmd = Command::cargo_bin("csv-combiner-rs").unwrap();
    cmd.arg("-t").arg("4").assert().failure();
  }

  #[test]
  fn fails_when_files_dont_exist() {
    let mut cmd = Command::cargo_bin("csv-combiner-rs").unwrap();
    cmd.arg("nonexistent_file.txt").arg("another_one.txt").assert().failure();
  }

  #[test]
  fn combines_single_file() {
    let mut cmd = Command::cargo_bin("csv-combiner-rs").unwrap();
    let output = cmd.arg("fixtures/accessories.csv").assert().get_output().stdout.clone();
    let output_string = String::from_utf8(output).unwrap();
    let mut output_lines = output_string.lines();
    let file = File::open("fixtures/accessories.csv").unwrap();
    let mut file_lines = BufReader::new(file).lines();
    assert_eq!(format!("{},\"filename\"", file_lines.next().unwrap().unwrap()), output_lines.next().unwrap());
    loop {
      match file_lines.next() {
        None => break,
        Some(file_line) => {
          assert_eq!(format!("{},\"accessories.csv\"", file_line.unwrap()), output_lines.next().unwrap());
        }
      }
    }
  }

  fn combines_multiples_files_base(threads: &str) {
    let mut cmd = Command::cargo_bin("csv-combiner-rs").unwrap();
    let output = cmd
      .arg("fixtures/accessories.csv")
      .arg("fixtures/clothing.csv")
      .arg("fixtures/household_cleaners.csv")
      .arg("-t").arg(threads)
      .assert().get_output().stdout.clone();
    let output_string = String::from_utf8(output).unwrap();
    let mut output_lines = output_string.lines();
    let files = [
      File::open("fixtures/accessories.csv").unwrap(),
      File::open("fixtures/clothing.csv").unwrap(),
      File::open("fixtures/household_cleaners.csv").unwrap()
    ];
    let mut file_lines = [
      ("accessories.csv", BufReader::new(&files[0]).lines().peekable()),
      ("clothing.csv", BufReader::new(&files[1]).lines().peekable()),
      ("household_cleaners.csv", BufReader::new(&files[2]).lines().peekable())
    ];
    assert_eq!(format!("{},\"filename\"", file_lines[0].1.next().unwrap().unwrap()), output_lines.next().unwrap());
    file_lines[1].1.next();
    file_lines[2].1.next();
    'outer: loop {
      match output_lines.next() {
        None => break,
        Some(output_line) => {
          for (filename, fl) in &mut file_lines {
            if let None = fl.peek() {
              continue;
            }
            if output_line.eq(format!("{},\"{}\"", fl.peek().unwrap().as_deref().unwrap(), filename).as_str()) {
              fl.next();
              continue 'outer;
            }
          }
          panic!("Mismatched line: {}", output_line);
        }
      }
    }
  }

  #[test]
  fn combines_multiples_files() {
    combines_multiples_files_base("64");
  }

  #[test]
  fn combines_multiples_files_with_thread_limit() {
    combines_multiples_files_base("2");
  }
}