//! Data that is not in a local file: object stores and their logins, HTTP, downloads
//! and local copies.

#[cfg(feature = "cloud")]
pub mod aws_profiles;
#[cfg(feature = "cloud")]
pub mod azure;
#[cfg(feature = "cloud")]
pub(crate) mod cloud_arrow;
#[cfg(feature = "cloud")]
pub mod cloud_browse;
#[cfg(feature = "cloud")]
pub mod cloud_command;
pub mod cloud_env;
#[cfg(feature = "cloud")]
pub(crate) mod cloud_hive;
#[cfg(feature = "cloud")]
pub mod cloud_sources;
pub mod download;
#[cfg(feature = "cloud")]
pub mod gcloud;
pub mod local_copy;
#[cfg(any(feature = "http", feature = "cloud"))]
pub(crate) mod remote_model;
#[cfg(feature = "cloud")]
pub mod s3_tools;
pub mod source;
pub mod user_agent;
