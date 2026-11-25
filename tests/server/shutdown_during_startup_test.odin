#+build linux
package server_tests

import "core:net"
import "core:sync"
import "core:testing"
import "core:time"

import http "../.."

// server_shutdown runs as serve spawns thread 2: thread 1 holds its event loop and waits for serve
// to finish spawning, and no thread has published a loop yet.
shutdown_during_startup :: proc(t: ^testing.T, before: Process_Resources) {
	server := http.Server{}
	opts := http.Default_Server_Opts
	opts.thread_count = 4
	listen_error := http.listen(&server, {address = net.IP4_Loopback, port = 0}, opts)
	if !testing.expectf(t, listen_error == nil, "listen: %v", listen_error) {
		return
	}

	serve_error: http.Server_Error
	spawns := Thread_Spawns{backing = context.allocator, count = 0, shutdown = Shutdown_At_Spawn{server = &server, spawn = 2}}
	{
		context.allocator = thread_spawns_allocator(&spawns)
		serve_error = http.serve(&server, http.handler(proc(_: ^http.Request, _: ^http.Response) {}))
	}

	testing.expect_value(t, serve_error, nil)
	expect_resources_released(t, before)
}

@(test)
a_shutdown_during_startup_stops_every_thread :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, time.Second * 10)
	sync.mutex_lock(&process_resources_lock)
	defer sync.mutex_unlock(&process_resources_lock)
	defer free_all(context.temp_allocator)
	switch before in read_process_resources() {
	case Unreadable_Resources:
		testing.expectf(t, false, "process resources before listen: %v", before)
	case Process_Resources:
		shutdown_during_startup(t, before)
	}
}
