#[derive(Debug, Clone)]
pub struct ErrorTracker {
    pub heartbeat: Vec<tonic::Status>,
    pub task_puller: Vec<tonic::Status>,
}

impl Default for ErrorTracker {
    fn default() -> Self {
        Self {
            heartbeat: vec![],
            task_puller: vec![],
        }
    }
}
