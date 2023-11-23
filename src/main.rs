use clap::{arg, command, value_parser, Command};
use fs_extra::dir::get_size;
use futures::future::TryJoinAll;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::cmp;
use std::ops::Range;
use std::path::PathBuf;
use std::time::Instant;
use tokio::fs::{create_dir_all, rename, File};
use tokio::io::{self, AsyncWriteExt};

#[tokio::main]
async fn main() -> io::Result<()> {
    let matches = command!()
        .subcommand(
            Command::new("make")
                .about("makes new files")
                .arg(
                    arg!(
                        -s --size <FILE> "Size of files to create in MB"
                    )
                    .default_value("128")
                    .value_parser(value_parser!(usize)),
                )
                .arg(
                    arg!(
                        -n --num <FILE> "Number of files to create"
                    )
                    .default_value("100")
                    .value_parser(value_parser!(usize)),
                )
                .arg(
                    arg!([dirname] "Destination directory")
                        .value_parser(value_parser!(PathBuf))
                        .default_value("data"),
                ),
        )
        .subcommand(
            Command::new("move")
                .about("move some files")
                .arg(
                    arg!([source] "Copy files from here")
                        .value_parser(value_parser!(PathBuf))
                        .default_value("data"),
                )
                .arg(
                    arg!([destination] "... to here")
                        .value_parser(value_parser!(PathBuf))
                        .default_value("new_data"),
                ),
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("move") {
        let source = matches.get_one::<PathBuf>("source").unwrap();
        let destination = matches.get_one::<PathBuf>("destination").unwrap();
        println!("Moving files from {:?} to {:?}", source, destination);
        let before = Instant::now();
        match rename(source, destination).await {
            Ok(_) => {
                let after = Instant::now();
                let folder_size = get_size(destination).unwrap();

                println!(
                    "Moved {} MB in {:?}",
                    folder_size / 1024 / 1024,
                    after.duration_since(before)
                )
            }
            Err(e) => println!("Error moving files: {:?}", e),
        }
    }

    if let Some(matches) = matches.subcommand_matches("make") {
        let num_megabytes = matches.get_one::<usize>("size").unwrap();
        let num = matches.get_one::<usize>("num").unwrap();
        let default_path = PathBuf::from("data");
        let dirname = matches
            .get_one::<PathBuf>("dirname")
            .unwrap_or(&default_path);

        create_dir_all(dirname).await?;
        println!("Making {} files of size {} MB", num, num_megabytes);
        let before: Instant = Instant::now();
        let handlers = Range {
            start: 1,
            end: *num,
        }
        .map(|i| tokio::spawn(write_bytes(i, *num_megabytes)))
        .collect::<Vec<_>>();

        let handle = handlers.into_iter().collect::<TryJoinAll<_>>().await;
        let after = Instant::now();

        if let Ok(totals) = handle {
            let (sums, _errors) = totals.into_iter().partition::<Vec<_>, _>(|x| x.is_ok());
            let total = sums.into_iter().map(|x| x.unwrap()).sum::<usize>();
            println!(
                "Total written: {} MB in {:?}",
                total / 1024 / 1024,
                after.duration_since(before)
            );
        }
    }

    Ok(())
}

async fn write_bytes(i: usize, num_megabytes: usize) -> io::Result<usize> {
    let mut rng: StdRng = SeedableRng::from_entropy();
    let mut buffer = [0; 1024];
    let total_bytes = num_megabytes * 1024 * 1024;
    let mut remaining_bytes = total_bytes;
    let file = File::create(format!("data/{:04}.dat", i)).await.unwrap();
    tokio::pin!(file);

    let before: Instant = Instant::now();
    while remaining_bytes > 0 {
        let to_write = cmp::min(remaining_bytes, buffer.len());
        let buffer = &mut buffer[..to_write];
        rng.fill(buffer);
        file.write_all(buffer).await?;

        remaining_bytes -= to_write;
    }
    let after = Instant::now();
    println!(
        "Wrote {} bytes in {:?}",
        total_bytes,
        after.duration_since(before)
    );
    Ok(total_bytes)
}
