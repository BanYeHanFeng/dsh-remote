pub const VERSION: &str = match option_env!("DSH_REMOTE_BUILD_VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};

pub mod actions;
pub mod agent;
pub mod call;
pub mod cli;
pub mod config;
pub mod exec;
pub mod hub;
pub mod key;
pub mod logging;
pub mod mcp;
pub mod mount;
pub mod net;
pub mod proto;
pub mod server;
pub mod session;
pub mod time;
