use tracing::info;

use crate::logger::initialize_logger;

pub mod logger;

fn main() {
    
    // dont drop this variable till the end of the program execution.
    let _guard = initialize_logger();
    info!("starting processing");
}

