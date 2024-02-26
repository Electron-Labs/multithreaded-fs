use std::{fs, num::NonZeroUsize, sync::{Arc, Mutex}, thread::available_parallelism, time::Instant};

use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize)]
struct Data {
    bytes: Vec<u8>,
}

fn get_no_avaiable_threads() -> NonZeroUsize {
     available_parallelism().unwrap()
}
// Read json data
pub fn read_bytes_from_json(json_path: &str) -> Vec<u8> {
    let json_data = fs::read_to_string(json_path).expect("Failed to read from file");
    
    // Deserialize json back to Vec<u8>
    let deserialized_data: Data = serde_json::from_str(&json_data).expect("Failed to deserialize data");
    deserialized_data.bytes
}

pub fn dump_bytes_to_json(bytes: Vec<u8>, json_path: &str) {
    // Serialize Vec<u8> to json
    let serialized_data = serde_json::to_string(&Data {
        bytes: bytes.clone(),
    })
    .expect("Failed to serialize data");
    // Write json to file
    fs::write(json_path, serialized_data).expect("Failed to write to file");
}

fn get_byte_split_length(total_byte_length: usize, no_of_split: usize) -> usize{
    let mut split_length = total_byte_length / no_of_split;
    if total_byte_length % no_of_split != 0 {
        split_length+=1;
    }
    split_length
}

fn get_byte_split_vec(data: Vec<u8>, no_of_split: usize) -> Vec<Vec<u8>> {
    let total_byte_length = data.len();
    let split_length = get_byte_split_length(total_byte_length, no_of_split);
    let mut remaining_length = total_byte_length;
    let mut start_index = 0;

    let mut bytes_sub_vec: Vec<Vec<u8>> = Vec::with_capacity(no_of_split);

    for i in 0..no_of_split {
        let current_split_length = std::cmp::min(split_length, remaining_length);
        let end_index = start_index + current_split_length;

        let slice = data[start_index..end_index].iter().cloned().collect();
        bytes_sub_vec.push(slice);
        remaining_length -= current_split_length;
        start_index = end_index;
        println!("Length of Vec {}: {}", i, bytes_sub_vec[i].len());
    };
    bytes_sub_vec
}

fn dump_file_bytes_splits(bytes_split_vec: Vec<Vec<u8>>, file_bytes_split_destination: String, file_name: String) {
    
    let avaiable_threads = get_no_avaiable_threads();
    let thread_count = std::cmp::min(bytes_split_vec.len(), avaiable_threads.into());
    let mut thrds = Vec::new();

        // TODO: fix this 
    for i in 0..thread_count {
        let vec = bytes_split_vec.get(i).unwrap().clone();
        let index = i; 
        let path: String = format!("{file_bytes_split_destination}_{file_name}_{index}_{i}.json");
        println!("dumping json  number {i}");
        thrds.push(
            std::thread::Builder::new().stack_size(209715200).spawn(move || {
                dump_bytes_to_json(vec, path.as_str())
            }).unwrap()
        );
    }
    for thrd in thrds {
        thrd.join().expect(&format!("Thread panicked"));
    }
}

pub fn file_preprocess(file_read_path: String, file_bytes_split_destination: String, no_of_file_split: usize, file_name: String) {
    let file_bytes = read_bytes_from_json(&file_read_path);
    let byte_split_vecs = get_byte_split_vec(file_bytes, no_of_file_split);
    dump_file_bytes_splits(byte_split_vecs, file_bytes_split_destination, file_name)
}

fn count_files_in_folder(folder_path: &str) -> Result<usize, std::io::Error> {
    let mut file_count = 0;
    let dir_entries = fs::read_dir(folder_path)?;
    for entry in dir_entries {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            file_count += 1;
        }
    }
    Ok(file_count)
}


pub fn read_file(file_read_folder: String, file_base_name: String) -> Vec<u8> {
    let mut thrds = Vec::new();
    let no_of_split = count_files_in_folder(&file_read_folder).unwrap();
    let available_threads = get_no_avaiable_threads();
    let mut byte_split_vecs: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::with_capacity(no_of_split)));
    let time = Instant::now();
    let thread_count = std::cmp::min(no_of_split, available_threads.into());
    
    for i in 0..thread_count {
        let divided_vecs_clone = Arc::clone(&byte_split_vecs);
        
        
        // TODO: fix these variable copying 
        let index = i;
        let file_read_folder1 =file_read_folder.clone();
        let file_base_name1 = file_base_name.clone();
        thrds.push(
            std::thread::Builder::new().stack_size(209715200).spawn(move || {
                // TODO: change the path
                let slice = read_bytes_from_json(format!("{}_{}_{index}.json", file_read_folder1, file_base_name1, index=index).as_str());
                let mut guard = divided_vecs_clone.lock().unwrap();
                // guard.push(slice);
                guard[i] = slice;
            }).unwrap()
        );  
    };

    for thrd in thrds {
        thrd.join().expect(&format!("Thread panicked"));
    }

    let timetaken = time.elapsed().as_secs(); 
    println!("time takne to read file : {:?}", timetaken);
    let mut result_vec: Vec<u8> = Vec::new();
    let guard = byte_split_vecs.lock().unwrap();
    for vec in guard.iter() {
        result_vec.extend_from_slice(&vec);
    }
    println!("final total len after reading proof: {:?}", result_vec.len());
    result_vec
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        file_preprocess(String::from("./json_data/json_file.json"), String::from("./preprocess"), 5, String::from("json_file"));
        // assert_eq!(result, 4);
    }
}
