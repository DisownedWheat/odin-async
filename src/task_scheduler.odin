package task_scheduler

import "base:runtime"
import "core:container/queue"
import "core:fmt"
import "core:log"
import "core:mem"
import "core:net"
import "core:os"
import "core:sync"
import "core:sync/chan"
import "core:thread"

MAX_TASKS_PER_THREAD: u32 = 10

Scheduler_Error :: enum {
	Dead_Scheduler,
}

Schedule_Parent :: union {
	^Scheduler,
	^Scheduler_Task,
}

Task_Process :: proc(task: ^Scheduler_Task, d: rawptr)

@(private)
Scheduler_Thread :: struct {
	tasks:        queue.Queue(^Scheduler_Task),
	current_task: ^Scheduler_Task,
	own_thread:   ^thread.Thread,
	scheduler:    ^Scheduler,
	lock:         sync.Mutex,
	id:           int,
	channel:      ^chan.Chan(Scheduler_Msg, .Send),
}

Scheduler :: struct {
	threads:              [dynamic]^Scheduler_Thread,
	// tasks_waiting:        [dynamic]^Scheduler_Task,
	tasks_waiting:        queue.Queue(Scheduler_Task),
	open_threads:         int,
	own_thread:           ^thread.Thread,
	max_tasks_per_thread: u32,
	allocator:            mem.Allocator,
	lock:                 sync.Mutex,
	task_id:              u64,
	wg:                   sync.Wait_Group,
	channel:              chan.Chan(Scheduler_Msg),
	alive:                bool,
}

Scheduler_Task :: struct {
	id:      u64,
	process: Task_Process,
	data:    rawptr,
	thread:  ^Scheduler_Thread,
	wg:      ^sync.Wait_Group,
}

@(private)
push_task :: proc(scheduler: ^Scheduler, task: ^Scheduler_Task) {
	task.id = scheduler.task_id
	scheduler.task_id += 1
	queue.push_back(&scheduler.tasks_waiting, task^)
}

task_await :: proc(parent: Schedule_Parent, process: Task_Process) -> Scheduler_Error {

	switch t in parent {
	case ^Scheduler_Task:
		t_thread := t.thread
		scheduler := t_thread.scheduler
		if !scheduler.alive {
			return .Dead_Scheduler
		}

		new_task := new(Scheduler_Task, scheduler.allocator)
		new_task.process = process
		new_task.wg = new(sync.Wait_Group)
		t.wg = new_task.wg
		sync.wait_group_add(new_task.wg, 1)

		sync.lock(&scheduler.lock)
		defer sync.unlock(&scheduler.lock)
		push_task(scheduler, new_task)
		sync.wait_group_wait(t.wg)
	case ^Scheduler:
		scheduler := t
		if !scheduler.alive {
			return .Dead_Scheduler
		}

		new_task := new(Scheduler_Task, scheduler.allocator)
		new_task.process = process
		new_task.wg = new(sync.Wait_Group)
		sync.wait_group_add(&scheduler.wg, 1)

		sync.lock(&scheduler.lock)
		defer sync.unlock(&scheduler.lock)
		push_task(scheduler, new_task)
	}
	return nil
}

task_spawn :: proc(parent: Schedule_Parent, process: Task_Process) -> Scheduler_Error {
	switch t in parent {
	case ^Scheduler_Task:
		t_thread := t.thread
		scheduler := t_thread.scheduler

		if !scheduler.alive {
			return .Dead_Scheduler
		}

		new_task := new(Scheduler_Task, scheduler.allocator)
		new_task.process = process

		sync.lock(&scheduler.lock)
		defer sync.unlock(&scheduler.lock)
		push_task(scheduler, new_task)
	case ^Scheduler:
		scheduler := t

		if !scheduler.alive {
			return .Dead_Scheduler
		}

		new_task := new(Scheduler_Task, scheduler.allocator)
		new_task.process = process

		sync.lock(&scheduler.lock)
		defer sync.unlock(&scheduler.lock)
		push_task(scheduler, new_task)
	}
	return nil
}

@(private)
worker_thread :: proc(t: ^thread.Thread) {
	state: ^Scheduler_Thread = (^Scheduler_Thread)(t.data)
	scheduler := state.scheduler
	sync.wait_group_add(&scheduler.wg, 1)
	defer sync.wait_group_done(&scheduler.wg)

	for {
		sync.lock(&state.lock)
		task, ok := queue.pop_front_safe(&state.tasks)
		sync.unlock(&state.lock)
		if !ok {
			chan.send(state.channel^, Worker_Complete{id = state.id})
			return
		}
		defer free(task)
		task.process(task, task.data)
	}
}

scheduler_join :: proc(scheduler: ^Scheduler) -> Scheduler_Error {
	if !scheduler.alive {
		return .Dead_Scheduler
	}

	threads := make([dynamic]^thread.Thread)
	defer delete(threads)
	for t in scheduler.threads {
		if t != nil {
			append(&threads, t.own_thread)
		}
	}
	thread.join_multiple(..threads[:])
	thread.join(scheduler.own_thread)
	scheduler_free(scheduler)
	return nil
}

@(private)
scheduler_free :: proc(scheduler: ^Scheduler) {
	defer free(scheduler)
	for &t in scheduler.threads {
		sync.mutex_lock(&t.lock)
		defer sync.mutex_unlock(&t.lock)
		thread.terminate(t.own_thread, 0)
	}
	delete(scheduler.threads)
	thread.terminate(scheduler.own_thread, 0)
	free(scheduler.own_thread)
}

@(private)
schedule_process :: proc(data: ^thread.Thread) {
	scheduler: ^Scheduler
	scheduler = (^Scheduler)(data.data)
	defer sync.wait_group_wait(&scheduler.wg)

	for {
		log.info("Waiting msg...")
		msg, ok := chan.recv(scheduler.channel)
		if !ok {
			break
		}
		sync.lock(&scheduler.lock)
		defer sync.unlock(&scheduler.lock)
	}
}

scheduler_init :: proc(
	thread_count: Maybe(int),
	allocator: mem.Allocator = context.allocator,
	max_tasks_per_thread: u32 = MAX_TASKS_PER_THREAD,
) -> (
	Scheduler,
	mem.Allocator_Error,
) {
	fmt.println("Starting")
	lock: sync.Mutex
	scheduler, scheduler_err := new(Scheduler)
	fmt.println("Scheduler created")
	if scheduler_err != nil {
		return scheduler^, scheduler_err
	}

	max_threads := thread_count.? or_else os.processor_core_count()
	threads := make([dynamic]^Scheduler_Thread, allocator)
	reserve_err := reserve(&threads, max_threads)
	if reserve_err != nil {
		return scheduler^, reserve_err
	}

	scheduler.threads = threads
	scheduler.open_threads = 0
	scheduler.max_tasks_per_thread = max_tasks_per_thread
	scheduler.alive = true

	channel, err := chan.create_buffered(chan.Chan(Scheduler_Msg), max_threads * 2, allocator)
	if err != nil {
		return scheduler^, err
	}
	scheduler.channel = channel

	own_thread := thread.create(schedule_process)
	own_thread.data = &scheduler
	scheduler.own_thread = own_thread

	initial_task := Scheduler_Task{}

	thread.start(own_thread)
	return scheduler^, nil
}
