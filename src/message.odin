package task_scheduler

Scheduler_Msg :: union {
	Task_Complete,
	Task_Added,
	Worker_Complete,
	Shutdown,
}

Task_Complete :: struct {
}

Task_Added :: struct {
}

Worker_Complete :: struct {
	id: int,
}

Shutdown :: struct {
}
