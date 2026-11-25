#+build linux
package server_tests

import "core:mem"
import "core:net"
import "core:sync"
import "core:testing"
import "core:thread"
import "core:time"

import http "../.."

Separate_Allocators_Run :: struct #all_or_none {
	server:         http.Server,
	observer:       Serving_Observer,
	listen_tracker: mem.Tracking_Allocator,
	serve_tracker:  mem.Tracking_Allocator,
	returned:       Maybe(Server_Return),
}

// listen runs with `listen_tracker` as the context allocator and serve with `serve_tracker`.
serve_with_separate_allocators :: proc(run: ^Separate_Allocators_Run) {
	context.logger = serving_logger(&run.observer)
	defer sync.one_shot_event_signal(&run.observer.settled)

	context.allocator = mem.tracking_allocator(&run.listen_tracker)
	opts := http.Default_Server_Opts
	opts.thread_count = 1
	if listen_error := http.listen(&run.server, {address = net.IP4_Loopback, port = 0}, opts); listen_error != nil {
		run.returned = Listen_Failed{error = listen_error}
		return
	}

	context.allocator = mem.tracking_allocator(&run.serve_tracker)
	serve_error := http.serve(&run.server, http.handler(proc(_: ^http.Request, _: ^http.Response) {}))
	run.returned = Serve_Returned{error = serve_error}
}

@(test)
serve_frees_its_threads_with_the_allocator_that_made_them :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, time.Second * 10)
	sync.mutex_lock(&process_resources_lock)
	defer sync.mutex_unlock(&process_resources_lock)

	run := new(Separate_Allocators_Run)
	defer free(run)
	run.observer.threads = 1
	mem.tracking_allocator_init(&run.listen_tracker, context.allocator)
	defer mem.tracking_allocator_destroy(&run.listen_tracker)
	mem.tracking_allocator_init(&run.serve_tracker, context.allocator)
	defer mem.tracking_allocator_destroy(&run.serve_tracker)
	run.serve_tracker.bad_free_callback = mem.tracking_allocator_bad_free_callback_add_to_array

	server_thread := thread.create_and_start_with_poly_data(run, serve_with_separate_allocators)
	sync.one_shot_event_wait(&run.observer.settled)
	http.server_shutdown(&run.server)
	thread.join(server_thread)
	thread.destroy(server_thread)
	// A second shutdown, after serve returned.
	http.server_shutdown(&run.server)

	testing.expectf(t, run.returned == Server_Return(Serve_Returned{error = nil}), "serve returned %v", run.returned)
	testing.expect_value(t, len(run.serve_tracker.bad_free_array), 0)
	testing.expect_value(t, len(run.listen_tracker.allocation_map), 0)
}
