package test

import task "../src"
import "core:fmt"
import "core:log"
import "core:testing"
import "core:time"

@(test)
test_init :: proc(t: ^testing.T) {
	fmt.println(context.allocator)
	s, err := task.scheduler_init(nil, context.allocator)
	if err != nil {
		testing.fail(t)
	}
	task.task_spawn(&s, test_inner_proc)
	task.scheduler_join(&s)
}

test_inner_proc :: proc(t: ^task.Scheduler_Task, data: rawptr) {
	data: ^string = (^string)(data)
	task.task_await(t, test_inner_inner_proc)
	fmt.println(data^)
}

test_inner_inner_proc :: proc(t: ^task.Scheduler_Task, data: rawptr) {
	time.sleep(time.Second * 5)
	fmt.println("Done in here")
}
