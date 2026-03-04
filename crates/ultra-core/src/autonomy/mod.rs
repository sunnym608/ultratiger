use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct TaskItem {
    pub id: String,
    pub task_type: String,
    pub payload: String,
    pub attempts: u32,
    pub max_attempts: u32,
}

#[derive(Debug, Default)]
pub struct TaskQueue {
    pending: VecDeque<TaskItem>,
    dead_letter: Vec<TaskItem>,
}

impl TaskQueue {
    pub fn enqueue(&mut self, task: TaskItem) {
        self.pending.push_back(task);
    }

    pub fn dequeue(&mut self) -> Option<TaskItem> {
        self.pending.pop_front()
    }

    pub fn mark_failed(&mut self, mut task: TaskItem) {
        task.attempts += 1;
        if task.attempts >= task.max_attempts {
            self.dead_letter.push(task);
        } else {
            self.pending.push_back(task);
        }
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn dead_letter_count(&self) -> usize {
        self.dead_letter.len()
    }
}

#[derive(Debug, Clone)]
pub enum SchedulerTrigger {
    EverySeconds(u64),
    Cron(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_task_retries_then_dead_letters() {
        let mut queue = TaskQueue::default();
        let task = TaskItem {
            id: "t1".to_owned(),
            task_type: "sync".to_owned(),
            payload: "{}".to_owned(),
            attempts: 0,
            max_attempts: 2,
        };

        queue.enqueue(task);
        let t = queue.dequeue().expect("task exists");
        queue.mark_failed(t);
        assert_eq!(queue.pending_count(), 1);

        let t = queue.dequeue().expect("task exists after retry");
        queue.mark_failed(t);
        assert_eq!(queue.pending_count(), 0);
        assert_eq!(queue.dead_letter_count(), 1);
    }
}
