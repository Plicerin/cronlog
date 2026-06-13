use thiserror::Error;

pub type Result<T> = std::result::Result<T, Cron2Error>;

#[derive(Debug, Error)]
pub enum Cron2Error {
    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid schedule: {0}")]
    InvalidSchedule(String),

    #[error("invalid command: {0}")]
    InvalidCommand(String),

    #[error("not found: {0}")]
    NotFound(String),
}
