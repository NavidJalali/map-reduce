use crate::state::WorkerInfo;

#[derive(Debug, Clone, Copy)]
pub enum TaskState {
    Idle,
    InProgress { worker: WorkerInfo },
    Completed { worker: WorkerInfo },
}

pub struct TaskId(pub u32);

pub struct Task {
    pub id: TaskId,
    pub state: TaskState,
}
