pub mod types;

use std::{collections::HashMap, fs, sync::{Arc, Mutex}, time::Instant};

use serde::{Deserialize, Serialize};
use types::ByteHandler;
use std::path::Path;


// Internal type which handles reading and writing data to and fro sub-files
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct DumpData {
    bytes: Vec<u8>,
}

pub fn read_bytes_from_json(json_path: &str) -> Vec<u8> {
    let time = Instant::now();
    let json_data = fs::read_to_string(json_path).unwrap();
    // Deserialize json back to Vec<u8>
    let deserialized_data: DumpData = serde_json::from_str(&json_data).expect("Failed to deserialize data");
    println!("time to one chunk of file : {:?}", time.elapsed());
    deserialized_data.bytes
}

pub fn dump_bytes_to_json(bytes: &[u8], json_path: &str, i :usize) {
    // Serialize Vec<u8> to json
    let time = Instant::now();
    let serialized_data = serde_json::to_string(&DumpData {
        bytes: bytes.to_vec(),
    })
    .expect("Failed to serialize data");
    // Write json to file
    fs::write(json_path, serialized_data).expect("Failed to write to file");
    println!("time taken to dump {:?} th file data: {:?}", i, time.elapsed())
}

fn get_byte_split_length(total_byte_length: usize, split_count: usize) -> usize{
    // Accounts for total bytes that can be divided into a split_count number of splits
    let split_length = (total_byte_length) / split_count;
    split_length
}

fn make_folder_if_not_exist(path: &str, folder_name: &str) {
    // Construct the full path including the folder name
    let full_path = format!("{}/{}", path, folder_name);

    // Create the folder if it does not exist
    if let Err(err) = fs::create_dir_all(&full_path) {
        eprintln!("Error creating folder: {}", err);
    } else {
        println!("Folder created successfully at: {}", full_path);
    }
}

fn get_file_name_from_path(path: &str) -> &str { 
    let file_name_without_extension = Path::new(path)
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap();
    file_name_without_extension
}

pub fn process_file_bytes(file_bytes: &Vec<u8>, file_read_path: String, file_bytes_split_destination: String, no_of_file_split: usize) {
    println!("preprocess file byte func start");
    let mut write_tasks = Vec::new();
    let base_len = get_byte_split_length(file_bytes.len(), no_of_file_split);
    let extra = base_len + file_bytes.len()%no_of_file_split;

    let file_name = get_file_name_from_path(&file_read_path);
    println!("file name is :{:?}", file_name);
    make_folder_if_not_exist(&file_bytes_split_destination, &file_name);
    for i in 0..no_of_file_split-1 {
        let path: String = format!("{file_bytes_split_destination}/{file_name}/{i}.json");
        let chunk = file_bytes[(i*base_len)..((i+1)*base_len)].to_vec();
        write_tasks.push(
            std::thread::spawn(move || {
                dump_bytes_to_json(&chunk, path.as_str(), i)
            })
        );
    }
    let path: String = format!("{file_bytes_split_destination}/{file_name}/{}.json", no_of_file_split-1);
    let last_chunk= file_bytes[(file_bytes.len()-extra)..].to_vec();
    write_tasks.push(
        std::thread::spawn(move || {
            dump_bytes_to_json(&last_chunk, path.as_str(), no_of_file_split-1)
        })
    );

    for thrd in write_tasks {
        thrd.join().expect(&format!("Thread panicked"));
    }
}


pub fn file_preprocess<T: ByteHandler>(file_read_path: String, file_bytes_split_destination: String, no_of_file_split: usize) {
    let file_bytes: Vec<u8> = read_bytes_from_json(&file_read_path);
    process_file_bytes(&file_bytes, file_read_path, file_bytes_split_destination, no_of_file_split);
}

fn count_files_in_folder(folder_path: &str, file_name: &str) -> Result<usize, std::io::Error> {
    let mut file_count = 0;
    let full_path = format!("{folder_path}/{file_name}");
    let dir_entries = fs::read_dir(full_path)?;
    for entry in dir_entries {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            file_count += 1;
        }
    }

    println!("the file count is: {file_count}");
    Ok(file_count)
}

fn get_final_byte_vec(byte_split_map: Arc<Mutex<HashMap<usize, Vec<u8>>>>) -> Vec<u8> {
    let mut result_vec: Vec<u8> = Vec::new();
    let sorted_keys: Vec<_> = {
        let map = byte_split_map.lock().unwrap();
        let mut keys: Vec<_> = map.keys().cloned().collect();
        keys.sort();
        keys
    };

    for key in sorted_keys {
        let map = byte_split_map.lock().unwrap();
        if let Some(value) = map.get(&key) {
            result_vec.extend_from_slice(&value); 
        }
    }
    println!("final total len of final byte vec: {:?}", result_vec.len());
    result_vec
}


pub fn read_file<T: ByteHandler>(file_read_folder: String, file_name: String) -> T {
    let mut thrds = Vec::new();
    let no_of_split = count_files_in_folder(&file_read_folder, &file_name).unwrap();
    let byte_split_map: Arc<Mutex<HashMap<usize, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
    let time: Instant = Instant::now();
    for i in 0..no_of_split {
        let byte_split_map_clone = Arc::clone(&byte_split_map);
        let file_read_path = format!("{file_read_folder}/{file_name}/{i}.json");
        thrds.push(
            std::thread::Builder::new().spawn(move || {
                let slice  = read_bytes_from_json(&file_read_path);
                let mut guard = byte_split_map_clone.lock().unwrap();
                guard.insert(i, slice);
            }).unwrap()
        );  
    };

    for thrd in thrds {
        thrd.join().expect(&format!("Thread panicked"));
    }

    let timetaken = time.elapsed().as_secs(); 
    println!("time takne to read file : {:?}", timetaken);

   
    let res = T::from_bytes(get_final_byte_vec(byte_split_map));
    res
}


#[cfg(test)]
mod tests {
    use super::*;
    impl ByteHandler for DumpData{
        fn get_bytes(&self) -> Vec<u8> {
            self.bytes.clone()
        }
    
        fn from_bytes(a: Vec<u8>) -> Self {
            return DumpData{bytes: a}
        }
    }

    #[test]
    fn file_e2e_test() {
        // preprocess and deconstruct file
        file_preprocess::<DumpData>(String::from("./json_data/json_file.json"), String::from("./preprocess"), 13);
        // read deconstructed file into bytes
        let data = read_file::<DumpData>(String::from("./preprocess"), String::from("json_file"));
        // read original file into bytes
        let original_bytes = read_bytes_from_json(&String::from("./json_data/json_file.json"));
        assert_eq!(data.bytes, original_bytes);
    }
}
