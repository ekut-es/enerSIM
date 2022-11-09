//! This example can be used to simulate complete traderounds (with calculations of disposable
//! energy and battery states etc.) of a neighborhood. A single Neighborhood can be instantiated
//! by sending a list of JSON representations of [HouseholdDescription] to the [create] Method.
//! The Mosaik-Interface is different from [marketplace_sim](marketplace_sim). The
use enerdagsim::HouseholdBatterySim;
use log::*;
use mosaik_rust_api::{run_simulation, ConnectionDirection};
use clap::Parser;

///Read, if we get an address or not
#[derive(Parser, Debug)]
struct Args {
    //The local addres mosaik connects to or none, if we connect to them
    #[clap(short = 'a', long)]
    addr: Option<String>,
}

pub fn main() /*-> Result<()>*/
{
    //get the address if there is one
    let opt = Args::parse();
    env_logger::init();

    let address = match opt.addr {
        //case if we connect us to mosaik
        Some(mosaik_addr) => ConnectionDirection::ConnectToAddress(
            mosaik_addr.parse().expect("Address is not parseable."),
        ),
        //case if mosaik connects to us
        None => {
            let addr = "127.0.0.1:3456";
            ConnectionDirection::ListenOnAddress(addr.parse().expect("Address is not parseable."))
        }
    };

    //initialize the simulator.
    let simulator = HouseholdBatterySim::init_sim();
    //start build_connection in the library.
    if let Err(e) = run_simulation(address, simulator) {
        error!("{:?}", e);
    }
}
