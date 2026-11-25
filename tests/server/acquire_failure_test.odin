#+build linux
package server_tests

import "core:mem"
import "core:nbio"
import "core:net"
import "core:sync"
import "core:sys/linux"
import "core:testing"
import "core:thread"
import "core:time"

import http "../.."

// Server thread 1 acquires its event loop, thread 2 finds no free descriptor, and thread 3 is
// never spawned.
LOOPS_ACQUIRED :: 1
THREAD_COUNT :: LOOPS_ACQUIRED + 3

Loop_Descriptors :: struct #all_or_none {
	count: int,
}

Loop_Descriptors_Measure :: union #no_nil {
	Loop_Descriptors,
	Unreadable_Resources,
	nbio.General_Error,
}

// Runs on a new thread, which holds no event loop reference, so the acquire creates a loop.
measure_loop_descriptors :: proc(measure: ^Loop_Descriptors_Measure) {
	defer free_all(context.temp_allocator)
	switch before in read_directory_entries("/proc/self/fd") {
	case Unreadable_Resources:
		measure^ = before
	case Directory_Entries:
		if acquire_error := nbio.acquire_thread_event_loop(); acquire_error != .None {
			measure^ = acquire_error
			return
		}
		defer nbio.release_thread_event_loop()
		switch after in read_directory_entries("/proc/self/fd") {
		case Unreadable_Resources:
			measure^ = after
		case Directory_Entries:
			measure^ = Loop_Descriptors{count = len(after.names) - len(before.names)}
		}
	}
}

@(require_results)
measure_loop_descriptors_on_new_thread :: proc() -> Loop_Descriptors_Measure {
	measure := new(Loop_Descriptors_Measure)
	defer free(measure)
	measuring := thread.create_and_start_with_poly_data(measure, measure_loop_descriptors)
	thread.join(measuring)
	thread.destroy(measuring)
	return measure^
}

serve_without_free_descriptors :: proc(t: ^testing.T, before: Process_Resources, loop: Loop_Descriptors) {
	tracker := mem.Tracking_Allocator{}
	mem.tracking_allocator_init(&tracker, context.allocator)
	defer mem.tracking_allocator_destroy(&tracker)
	context.allocator = mem.tracking_allocator(&tracker)

	server := http.Server{}
	opts := http.Default_Server_Opts
	opts.thread_count = THREAD_COUNT
	listen_error := http.listen(&server, {address = net.IP4_Loopback, port = 0}, opts)
	if !testing.expectf(t, listen_error == nil, "listen: %v", listen_error) {
		return
	}
	handler := http.handler(proc(_: ^http.Request, _: ^http.Response) {
		panic("a server whose threads did not all start cannot serve")
	})

	serve_error: http.Server_Error
	spawns := Thread_Spawns{backing = context.allocator, count = 0, shutdown = nil}
	switch restriction in restrict_descriptors(LOOPS_ACQUIRED * loop.count) {
	case Restriction_Failed:
		testing.expectf(t, false, "descriptor restriction: %v", restriction)
		http.server_shutdown(&server)
		_ = http.serve(&server, handler)
		return
	case Descriptors_Restricted:
		{
			context.allocator = thread_spawns_allocator(&spawns)
			serve_error = http.serve(&server, handler)
		}
		release_descriptors(restriction)
	}
	_, listener_after := linux.fcntl(linux.Fd(server.tcp_sock), linux.F_GETFD)

	testing.expect_value(t, serve_error, http.Server_Error(nbio.General_Error(linux.Errno.EMFILE)))
	testing.expect_value(t, listener_after, linux.Errno.EBADF)
	// An unjoined thread leaves its thread.Thread allocated.
	testing.expect_value(t, len(tracker.allocation_map), 0)
	// No thread is spawned after the one whose acquire failed.
	testing.expect_value(t, sync.atomic_load(&spawns.count), LOOPS_ACQUIRED + 1)
	expect_resources_released(t, before)
}

@(test)
serve_returns_a_thread_event_loop_failure_after_stopping_the_threads_it_started :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, time.Second * 10)
	sync.mutex_lock(&process_resources_lock)
	defer sync.mutex_unlock(&process_resources_lock)
	defer free_all(context.temp_allocator)
	switch before in read_process_resources() {
	case Unreadable_Resources:
		testing.expectf(t, false, "process resources before listen: %v", before)
	case Process_Resources:
		switch loop in measure_loop_descriptors_on_new_thread() {
		case Unreadable_Resources, nbio.General_Error:
			testing.expectf(t, false, "descriptors of one event loop: %v", loop)
		case Loop_Descriptors:
			serve_without_free_descriptors(t, before, loop)
		}
	}
}

Failing_Listen :: struct #all_or_none {
	restriction:      Maybe(Restriction_Failed),
	listen_error:     http.Server_Error,
	// Allocations listen made that were still live after it returned.
	allocations_left: int,
}

// Runs on a new thread: a failed acquire stays recorded in the thread's event loop state, and would
// fail every later acquire of a test runner thread.
listen_without_free_descriptors :: proc(listening: ^Failing_Listen) {
	tracker := mem.Tracking_Allocator{}
	mem.tracking_allocator_init(&tracker, context.allocator)
	defer mem.tracking_allocator_destroy(&tracker)
	context.allocator = mem.tracking_allocator(&tracker)

	server := http.Server{}
	switch restriction in restrict_descriptors(0) {
	case Restriction_Failed:
		listening.restriction = restriction
	case Descriptors_Restricted:
		listening.listen_error = http.listen(&server, {address = net.IP4_Loopback, port = 0})
		release_descriptors(restriction)
		if listening.listen_error == nil {
			http.server_shutdown(&server)
			_ = http.serve(&server, http.handler(proc(_: ^http.Request, _: ^http.Response) {}))
		}
	}
	listening.allocations_left = len(tracker.allocation_map)
}

@(test)
listen_returns_its_event_loop_failure_after_releasing_what_it_acquired :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, time.Second * 10)
	sync.mutex_lock(&process_resources_lock)
	defer sync.mutex_unlock(&process_resources_lock)
	defer free_all(context.temp_allocator)
	switch before in read_process_resources() {
	case Unreadable_Resources:
		testing.expectf(t, false, "process resources before listen: %v", before)
	case Process_Resources:
		listening := new_clone(Failing_Listen{restriction = nil, listen_error = nil, allocations_left = 0})
		defer free(listening)
		listen_thread := thread.create_and_start_with_poly_data(listening, listen_without_free_descriptors)
		thread.join(listen_thread)
		thread.destroy(listen_thread)

		if !testing.expectf(t, listening.restriction == nil, "descriptor restriction: %v", listening.restriction) {
			return
		}
		testing.expect_value(t, listening.listen_error, http.Server_Error(nbio.General_Error(linux.Errno.EMFILE)))
		testing.expect_value(t, listening.allocations_left, 0)
		expect_resources_released(t, before)
	}
}
