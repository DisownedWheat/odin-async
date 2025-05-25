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
	wg:           ^sync.Wait_Group,
	lock:         sync.Mutex,
	id:           int,
	channel:      chan.Chan(Scheduler_Msg, .Send),
}

Scheduler :: struct {
	threads:              [dynamic]^Scheduler_Thread,
	tasks_waiting:        queue.Queue(^Scheduler_Task),
	max_threads:          int,
	open_threads:         int,
	own_thread:           ^thread.Thread,
	max_tasks_per_thread: u32,
	allocator:            mem.Allocator,
	lock:                 sync.Mutex,
	task_id:              u64,
	wg:                   sync.Wait_Group,
	thread_wg:            ^sync.Wait_Group,
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
	queue.push_back(&scheduler.tasks_waiting, task)
}

@(private)
pop_task :: proc(scheduler: ^Scheduler) -> (^Scheduler_Task, bool) {
	sync.lock(&scheduler.lock)
	defer sync.unlock(&scheduler.lock)
	next, ok := queue.pop_front_safe(&scheduler.tasks_waiting)

	return next, ok
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
		chan.send(scheduler.channel, Task_Added{})
	case ^Scheduler:
		scheduler := t

		if !scheduler.alive {
			return .Dead_Scheduler
		}

		new_task := new(Scheduler_Task, scheduler.allocator)
		new_task.process = process
		fmt.println("New task created", new_task)

		sync.lock(&scheduler.lock)
		defer sync.unlock(&scheduler.lock)
		push_task(scheduler, new_task)

		chan.send(scheduler.channel, Task_Added{})
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
		fmt.println("Worker thread running...")
		sync.cond_wait(&state.wg.cond, &state.lock)
		task, ok := pop_task(scheduler)
		if !ok {
			chan.send(state.channel, Worker_Complete{id = state.id})
			return
		}
		task.process(task, task.data)
		if task.wg != nil {
			sync.wait_group_done(task.wg)
		}
		chan.send(state.channel, Task_Complete{})
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
scheduler_process :: proc(data: ^thread.Thread) {
	fmt.println("Starting scheduler rocess thread")
	scheduler: ^Scheduler
	scheduler = (^Scheduler)(data.data)
	defer sync.wait_group_wait(&scheduler.wg)
	fmt.println("SCHEDULER THREADS", scheduler.threads)

	first_run_flag := true

	for {

		if first_run_flag {
			first_run_flag = false
			sync.wait_group_done(&scheduler.wg)
		}

		// log.info("Waiting msg...")
		fmt.println("Waiting msg")
		chan_msg, ok := chan.recv(scheduler.channel)
		if !ok {
			break
		}
		fmt.println("Got a message!")
		sync.lock(&scheduler.lock)
		defer sync.unlock(&scheduler.lock)

		fmt.println("Here now", chan_msg)
		fmt.println(scheduler.threads)

		switch msg in chan_msg {
		case Task_Added:
			fmt.println("Task has been added", scheduler.threads[:])
			sync.cond_signal(&scheduler.thread_wg.cond)
		// new_thread_needed := true
		// for t in scheduler.threads[:] {
		// 	sync.lock(&t.lock)
		// 	defer sync.unlock(&t.lock)
		//
		// 	if queue.len(t.tasks) >= int(scheduler.max_tasks_per_thread) {
		// 		continue
		// 	}
		// 	new_thread_needed = false
		// }
		//
		// fmt.println(
		// 	"New thread needed:",
		// 	new_thread_needed,
		// 	len(scheduler.threads),
		// 	scheduler.max_threads,
		// )
		//
		// if new_thread_needed && len(scheduler.threads) < scheduler.max_threads {
		// 	worker_t := thread.create(worker_thread)
		// 	worker := new(Scheduler_Thread, scheduler.allocator)
		// 	worker.own_thread = worker_t
		// 	worker.scheduler = scheduler
		// 	worker.lock = sync.Mutex{}
		// 	worker.channel = chan.as_send(scheduler.channel)
		//
		// 	queue_backer := new([10]^Scheduler_Task)
		// 	queue.init_from_slice(&worker.tasks, queue_backer[:])
		//
		// 	append(&scheduler.threads, worker)
		//
		// 	thread.start(worker.own_thread)
		// }
		case Shutdown:
			break
		case Task_Complete:
			fmt.println("Task complete, yay!")
		case Worker_Complete:
			fmt.println("Worker has finished")
		case:
			fmt.println("Something went wrong...")
		}
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
	scheduler := Scheduler{}

	max_threads := thread_count.? or_else os.processor_core_count()
	threads, reserve_err := make([dynamic]^Scheduler_Thread, allocator)
	if reserve_err != nil {
		return scheduler, reserve_err
	}

	thread_wg := new(sync.Wait_Group, allocator)
	for i in 0 ..< max_threads {
		fmt.println("Creating thread...")
		worker_t := thread.create(worker_thread)
		worker := new(Scheduler_Thread, scheduler.allocator)
		worker.own_thread = worker_t
		worker.scheduler = &scheduler
		worker.lock = sync.Mutex{}
		worker.channel = chan.as_send(scheduler.channel)
		worker.wg = thread_wg

		queue_backer := new([10]^Scheduler_Task)
		queue.init_from_slice(&worker.tasks, queue_backer[:])

		append(&scheduler.threads, worker)

		append(&threads, worker)
	}

	scheduler.open_threads = 0
	scheduler.max_tasks_per_thread = max_tasks_per_thread
	scheduler.alive = true
	scheduler.lock = sync.Mutex{}
	scheduler.allocator = allocator
	scheduler.wg = sync.Wait_Group{}
	scheduler.max_threads = max_threads
	scheduler.thread_wg = thread_wg

	channel, err := chan.create_buffered(chan.Chan(Scheduler_Msg), max_threads * 2, allocator)
	if err != nil {
		return scheduler, err
	}
	scheduler.channel = channel
	fmt.println("Channel created")

	own_thread := thread.create(scheduler_process)
	own_thread.data = &scheduler
	scheduler.own_thread = own_thread

	sync.wait_group_add(&scheduler.wg, 1)

	thread.start(own_thread)

	fmt.println("Waiting")
	sync.wait_group_wait(&scheduler.wg)
	fmt.println("DONE")

	for t in threads {
		thread.start(t.own_thread)
	}


	return scheduler, nil
}
