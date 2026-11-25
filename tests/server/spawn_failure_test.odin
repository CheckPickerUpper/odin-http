#+build linux
package server_tests

import "core:log"
import "core:mem"
import "core:net"
import "core:sync"
import "core:sys/linux"
import "core:testing"
import "core:thread"
import "core:time"

import http "../.."

// RLIMIT_NPROC counts every thread of this user, and this process alone runs more than one, so with a
// soft limit of 1 the kernel refuses every new thread of a task the limit applies to. It does not
// apply to root, or to a task with CAP_SYS_RESOURCE or CAP_SYS_ADMIN, so the test first spawns a
// thread of its own to find out whether the limit holds.
THREADS_ALLOWED :: 1

// Listens and serves twice on a server whose thread spawns all fail.
serve_while_spawns_fail :: proc(t: ^testing.T, before: Process_Resources) {
	server := http.Server{}
	opts := http.Default_Server_Opts
	opts.thread_count = 4
	listen_error := http.listen(&server, {address = net.IP4_Loopback, port = 0}, opts)
	if !testing.expectf(t, listen_error == nil, "listen: %v", listen_error) {
		return
	}
	handler := http.handler(proc(_: ^http.Request, _: ^http.Response) {
		panic("a server whose threads did not all start cannot serve")
	})

	serve_error := http.serve(&server, handler)
	second_spawns := Thread_Spawns{backing = context.allocator, count = 0}
	second_serve_error: http.Server_Error
	{
		context.allocator = thread_spawns_allocator(&second_spawns)
		second_serve_error = http.serve(&server, handler)
	}
	_, listener_after := linux.fcntl(linux.Fd(server.tcp_sock), linux.F_GETFD)

	testing.expect_value(t, serve_error, http.Server_Error(http.Server_Thread_Error.Spawn_Failed))
	testing.expect_value(t, listener_after, linux.Errno.EBADF)
	expect_resources_released(t, before)
	// A serve after the failed one returns at once and tries to spawn nothing.
	testing.expect_value(t, second_serve_error, nil)
	testing.expect_value(t, sync.atomic_load(&second_spawns.count), 0)
}

serve_without_thread_spawns :: proc(t: ^testing.T, before: Process_Resources) {
	tracker := mem.Tracking_Allocator{}
	mem.tracking_allocator_init(&tracker, context.allocator)
	defer mem.tracking_allocator_destroy(&tracker)
	context.allocator = mem.tracking_allocator(&tracker)

	switch lowering in lower_soft_limit(.NPROC, THREADS_ALLOWED) {
	case Limit_Not_Lowered:
		testing.expectf(t, false, "thread limit: %v", lowering)
	case Limit_Lowered:
		probe := thread.create_and_start(proc() {})
		if probe == nil {
			serve_while_spawns_fail(t, before)
		} else {
			thread.join(probe)
			thread.destroy(probe)
			log.warn("skipped: the kernel does not apply RLIMIT_NPROC to this task, so no thread spawn can fail")
		}
		restore_limit(lowering)
		testing.expect_value(t, len(tracker.allocation_map), 0)
	}
}

@(test)
serve_returns_a_thread_spawn_failure_after_releasing_what_listen_acquired :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, time.Second * 10)
	sync.mutex_lock(&process_resources_lock)
	defer sync.mutex_unlock(&process_resources_lock)
	defer free_all(context.temp_allocator)
	switch before in read_process_resources() {
	case Unreadable_Resources:
		testing.expectf(t, false, "process resources before listen: %v", before)
	case Process_Resources:
		serve_without_thread_spawns(t, before)
	}
}
