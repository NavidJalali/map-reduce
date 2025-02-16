use std::{
    fs,
    io::{self, BufRead, Write},
    vec,
};

fn wipe_chunks() -> io::Result<()> {
    let path = std::env::current_dir()?.join("chunkify").join("chunks");
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

fn chunkify() -> io::Result<usize> {
    let path = std::env::current_dir()?.join("chunkify").join("input.txt");
    let fstat = fs::metadata(&path)?;
    println!("Chunkifying file: {:?}", fstat);

    let file = fs::File::open(path)?;
    let reader = io::BufReader::new(file);
    let mut lines = reader.lines();

    let mut current_chunk = 0;
    let mut exhausted = false;
    loop {
        // open a new file for writing
        let path = std::env::current_dir()?
            .join("chunkify")
            .join("chunks")
            .join(format!("chunk-{}.txt", current_chunk));
        fs::create_dir_all(path.parent().unwrap())?;
        let file = fs::File::create(path)?;

        let mut writer = io::BufWriter::new(file);
        let mut lines_written = 0;

        // write 1000 lines to the file
        while lines_written < 1000 && !exhausted {
            if let Some(line) = lines.next() {
                let line = line?;
                if line.is_empty() {
                    continue;
                } else {
                    writer.write_all(line.as_bytes())?;
                    writer.write_all(b"\n")?;
                    lines_written += 1;
                }
            } else {
                exhausted = true;
                break;
            }
        }

        // close the file
        writer.flush()?;

        if exhausted {
            break Ok(current_chunk);
        }

        current_chunk += 1;
    }
}

fn replicate(chunks: usize, worker_count: usize, replication_factor: usize) {
    let base = std::env::current_dir().unwrap();

    let chunks_path = base.join("chunkify").join("chunks");
    let destination_path = base.join("input");

    (0..=chunks).for_each(|chunk| {
        let workers = if replication_factor == 1 {
            vec![chunk % worker_count]
        } else {
            let mut workers = Vec::with_capacity(replication_factor);
            workers.push(chunk % worker_count);
            for i in 1..replication_factor {
                workers.push((chunk + i) % worker_count);
            }
            workers
        };

        let file_name = format!("chunk-{}.txt", chunk);

        let source = chunks_path.join(file_name.clone());

        for worker in workers {
            let dest = destination_path
                .join(format!("worker-{}", worker))
                .join(file_name.clone());

            println!("Replicating {:?} to {:?}", source, dest);

            fs::copy(source.clone(), dest).unwrap();
        }
    });
}

fn main() -> io::Result<()> {
    wipe_chunks()?;
    let chunks = chunkify()?;
    println!("Chunkified into {} chunks", chunks);
    replicate(chunks, 3, 2);
    Ok(())
}
