use crate::error_tracker::ErrorTracker;

#[derive(Debug)]
pub enum ShutdownReason {
    Done,
    MasterError(Option<String>),
    WorkerError(ErrorTracker),
}
