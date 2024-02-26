pub mod types;

use std::{collections::HashMap, fs, sync::{Arc, Mutex}, time::Instant};

use serde::{Deserialize, Serialize};
use types::ByteHandler;

// Internal type which handles reading and writing data to and fro sub-files
#[derive(Serialize, Deserialize, Clone)]
struct DumpData {
    bytes: Vec<u8>,
}

// Read json data
pub fn read_bytes_from_json<T: Deserialize<'static> + ByteHandler>(json_path: &str) -> Vec<u8> {
    let json_data = fs::read_to_string(json_path).expect("Failed to read from file");
    
    // Deserialize json back to Vec<u8>
    let deserialized_data: T = serde_json::from_str(&json_data).expect("Failed to deserialize data");
    deserialized_data.get_bytes()
}

pub fn dump_bytes_to_json(bytes: &[u8], json_path: &str) {
    // Serialize Vec<u8> to json
    let serialized_data = serde_json::to_string(&DumpData {
        bytes: bytes.to_vec(),
    })
    .expect("Failed to serialize data");
    // Write json to file
    fs::write(json_path, serialized_data).expect("Failed to write to file");
}

fn get_byte_split_length(total_byte_length: usize, split_count: usize) -> usize{
    // Accounts for total bytes that can be divided into a split_count number of splits
    let mut split_length = total_byte_length / split_count;
    // Accounts for the remaining bytes that cannot be divided into a split_count number of splits
    if total_byte_length % split_count != 0 {
        split_length+=1;
    }
    split_length
}

// fn get_byte_split_vec(data: Vec<u8>, no_of_split: usize) -> Vec<Vec<u8>> {
//     let total_byte_length = data.len();
//     let split_length = get_byte_split_length(total_byte_length, no_of_split);
//     let mut remaining_length = total_byte_length;
//     let mut start_index = 0;

//     let mut bytes_sub_vec: Vec<Vec<u8>> = Vec::with_capacity(no_of_split);

//     for i in 0..no_of_split {
//         let current_split_length = std::cmp::min(split_length, remaining_length);
//         let end_index = start_index + current_split_length;

//         let slice = data[start_index..end_index].iter().cloned().collect();
//         bytes_sub_vec.push(slice);
//         remaining_length -= current_split_length;
//         start_index = end_index;
//         println!("Length of Vec {}: {}", i, bytes_sub_vec[i].len());
//     };
//     bytes_sub_vec
// }

fn make_folder_if_not_exist(path: String, folder_name: String) {
    // Construct the full path including the folder name
    let full_path = format!("{}/{}", path, folder_name);

    // Create the folder if it does not exist
    if let Err(err) = fs::create_dir_all(&full_path) {
        eprintln!("Error creating folder: {}", err);
    } else {
        println!("Folder created successfully at: {}", full_path);
    }
}

// fn dump_file_bytes_splits(bytes_split_vec: Vec<Vec<u8>>, file_bytes_split_destination: String, file_name: String) {
//     make_folder_if_not_exist(file_bytes_split_destination.clone(), file_name.clone());
//     let thread_count = bytes_split_vec.len();
//     let mut thrds = Vec::new();

//         // TODO: fix this 
//     for i in 0..thread_count {
//         let vec = bytes_split_vec.get(i).unwrap().clone();
//         let path: String = format!("{file_bytes_split_destination}/{file_name}/{i}.json");
//         thrds.push(
//             // TODO: reduce the stack size
//             std::thread::Builder::new().stack_size(209715200).spawn(move || {
//                 dump_bytes_to_json(vec, path.as_str())
//             }).unwrap()
//         );
//     }
//     for thrd in thrds {
//         thrd.join().expect(&format!("Thread panicked"));
//     }
// }

pub fn file_preprocess<T: Deserialize<'static> + ByteHandler>(file_read_path: String, file_bytes_split_destination: String, no_of_file_split: usize, file_name: String) {
    let file_bytes = read_bytes_from_json::<T>(&file_read_path);
    let mut write_tasks = Vec::new();

    let base_len = file_bytes.len()/no_of_file_split;
    let extra = file_bytes.len()%no_of_file_split;

    for i in 0..no_of_file_split-1 {
        let path: String = format!("{file_bytes_split_destination}/{file_name}/{i}.json");
        write_tasks.push(
            std::thread::spawn(move || {
                dump_bytes_to_json(&file_bytes[(i*base_len)..((i+1)*base_len)], path.as_str())
            })
        );
    }
    let path: String = format!("{file_bytes_split_destination}/{file_name}/{}.json", no_of_file_split-1);
    if extra != 0 {
        write_tasks.push(
            std::thread::spawn(move || {
                dump_bytes_to_json(&file_bytes[(file_bytes.len()-extra)..], path.as_str())
            })
        )
    } else {
        write_tasks.push(
            std::thread::spawn(move || {
                dump_bytes_to_json(&file_bytes[(file_bytes.len()-base_len)..], path.as_str())
            })
        )
    }
    // let byte_split_vecs = get_byte_split_vec(file_bytes, no_of_file_split);
    // dump_file_bytes_splits(byte_split_vecs, file_bytes_split_destination, file_name)
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


pub fn read_file(file_read_folder: String, file_name: String) -> Vec<u8> {
    let mut thrds = Vec::new();
    let no_of_split = count_files_in_folder(&file_read_folder, &file_name).unwrap();
    let byte_split_map: Arc<Mutex<HashMap<usize, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
    let time: Instant = Instant::now();
    for i in 0..no_of_split {
        let byte_split_map_clone = Arc::clone(&byte_split_map);

        let file_read_path = format!("{file_read_folder}/{file_name}/{i}.json");
        thrds.push(
            std::thread::Builder::new().stack_size(209715200).spawn(move || {
                let slice = read_bytes_from_json(&file_read_path);
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

    get_final_byte_vec(byte_split_map)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        file_preprocess(String::from("./json_data/json_file.json"), String::from("./preprocess"), 10 , String::from("json_file"));
        // assert_eq!(result, 4);
    }

    #[test]
    fn read_file_test() {
        let bytes = read_file(String::from("./preprocess"), String::from("json_file"));
        println!("the file bytes are: {:?}", bytes);
        // assert_eq!(result, 4);
    }
}
