#+build linux
package shutdown_wake

import "base:runtime"

import "core:c/libc"
import "core:log"
import "core:mem"
import "core:nbio"
import "core:net"
import "core:sync"
import "core:sys/linux"
import "core:testing"
import "core:thread"
import "core:time"

import http "../.."

Shutdown_Request :: enum {
	Direct,
	Interrupt,
}

@(private)
shutdown_before_serve :: proc(t: ^testing.T, shutdown_request: Shutdown_Request) {
	acquire_error := nbio.acquire_thread_event_loop()
	if !testing.expectf(t, acquire_error == nil, "caller event loop: %v (%d)", acquire_error, acquire_error) {
		return
	}
	server := new_clone(http.Server{})
	defer free(server)
	listen_error := http.listen(server, {address = net.IP4_Loopback, port = 0})
	assert(listen_error == nil)
	endpoint, endpoint_error := net.bound_endpoint(server.tcp_sock)
	assert(endpoint_error == nil)

	switch shutdown_request {
	case .Direct:
		http.server_shutdown(server)
	case .Interrupt:
		previous := libc.signal(libc.SIGINT, proc "c" (_: i32) {})
		defer libc.signal(libc.SIGINT, previous)
		http.server_shutdown_on_interrupt(server)
		assert(libc.raise(libc.SIGINT) == 0)
	}
	refused_socket, dial_error := net.dial_tcp(endpoint)
	if dial_error == nil {
		net.close(refused_socket)
	}
	testing.expect_value(t, dial_error, net.Dial_Error.Refused)

	serve_error := http.serve(server, http.handler(proc(_: ^http.Request, _: ^http.Response) {
		panic("a server stopped before serving cannot handle a request")
	}))
	testing.expect_value(t, serve_error, nil)
	_, descriptor_error := linux.fcntl(linux.Fd(server.tcp_sock), linux.F_GETFD)
	testing.expectf(t, descriptor_error == .EBADF, "listener descriptor after %v shutdown: %v", shutdown_request, descriptor_error)
	testing.expect(t, nbio.current_thread_event_loop() != nil, "serve must preserve the caller's loop reference")
	nbio.release_thread_event_loop()
	testing.expect(t, nbio.current_thread_event_loop() == nil, "serve must release the listener's loop reference")
	// Repeated serve calls on a stopped server cannot release another owner's loop.
	assert(nbio.acquire_thread_event_loop() == nil)
	defer nbio.release_thread_event_loop()
	testing.expect_value(t, http.serve(server, {}), nil)
	testing.expect(t, nbio.current_thread_event_loop() != nil)
}

Thread_Startup_Gate :: struct #all_or_none {
	allocator:               mem.Allocator,
	creator_id:              int,
	allocations_until_pause: int,
	entered:                 sync.One_Shot_Event,
	resume:                  sync.One_Shot_Event,
	loop_published:          sync.One_Shot_Event,
}

Serving_Startup :: struct #all_or_none {
	server:       http.Server,
	gate:         Thread_Startup_Gate,
	endpoint:     net.Endpoint,
	listen_error: net.Network_Error,
	serve_error:  net.Network_Error,
}

pause_thread_allocation :: proc(
	allocator_data: rawptr,
	mode: runtime.Allocator_Mode,
	size, alignment: int,
	old_memory: rawptr,
	old_size: int,
	location: runtime.Source_Code_Location = #caller_location,
) -> ([]u8, runtime.Allocator_Error) {
	gate := cast(^Thread_Startup_Gate)allocator_data
	switch mode {
	case .Alloc, .Alloc_Non_Zeroed:
		if size == size_of(thread.Thread) && sync.current_thread_id() == gate.creator_id && gate.allocations_until_pause > 0 {
			gate.allocations_until_pause -= 1
			if gate.allocations_until_pause == 0 {
				sync.one_shot_event_signal(&gate.entered)
				sync.one_shot_event_wait(&gate.resume)
			}
		}
	case .Free, .Free_All, .Resize, .Query_Features, .Query_Info, .Resize_Non_Zeroed:
	}
	return gate.allocator.procedure(
		gate.allocator.data,
		mode,
		size,
		alignment,
		old_memory,
		old_size,
		location,
	)
}

observe_loop_publication :: proc(
	data: rawptr,
	_: log.Level,
	message: string,
	_: log.Options,
	_: runtime.Source_Code_Location = #caller_location,
) {
	// The native startup message precedes the first tick of its published loop.
	if message == "starting event loop" {
		gate := cast(^Thread_Startup_Gate)data
		sync.one_shot_event_signal(&gate.loop_published)
	}
}

serve_startup :: proc(startup: ^Serving_Startup) {
	opts := http.Default_Server_Opts
	opts.thread_count = 3
	startup.listen_error = http.listen(&startup.server, {address = net.IP4_Loopback, port = 0}, opts)
	if startup.listen_error != nil {
		sync.one_shot_event_signal(&startup.gate.entered)
		return
	}

	endpoint, endpoint_error := net.bound_endpoint(startup.server.tcp_sock)
	assert(endpoint_error == nil)
	startup.endpoint = endpoint
	startup.gate.creator_id = sync.current_thread_id()
	// Thread creation pauses after serve allocates its thread slice. The listener
	// and all allocations still use the real heap allocator.
	context.allocator = mem.Allocator{procedure = pause_thread_allocation, data = &startup.gate}
	context.logger = log.Logger{
		procedure = observe_loop_publication,
		data = &startup.gate,
		lowest_level = .Debug,
		options = {},
	}
	startup.serve_error = http.serve(&startup.server, http.handler(proc(_: ^http.Request, res: ^http.Response) {
		http.respond_plain(res, "ready")
	}))
}

@(test)
shutdown_before_and_during_thread_startup_refuses_connections_and_finishes :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, time.Second * 5)
	// These cases share one test because signal handlers and descriptor allocation
	// belong to the process, so another startup case cannot reuse the closed fd.
	for shutdown_request in Shutdown_Request {
		shutdown_before_serve(t, shutdown_request)
	}
	for started_threads in 0..<2 {
		startup := Serving_Startup{
			server = {},
			gate = Thread_Startup_Gate{
				allocator = runtime.heap_allocator(),
				creator_id = 0,
				allocations_until_pause = started_threads + 1,
				entered = {},
				resume = {},
				loop_published = {},
			},
			endpoint = {},
			listen_error = nil,
			serve_error = nil,
		}
		server_thread := thread.create_and_start_with_poly_data(&startup, serve_startup)
		defer thread.destroy(server_thread)
		defer sync.one_shot_event_signal(&startup.gate.resume)

		sync.one_shot_event_wait(&startup.gate.entered)
		if !testing.expectf(t, startup.listen_error == nil, "listen: %v", startup.listen_error) {
			return
		}
		if started_threads > 0 {
			sync.one_shot_event_wait(&startup.gate.loop_published)
		}

		http.server_shutdown(&startup.server)
		socket, dial_error := net.dial_tcp(startup.endpoint)
		if dial_error == nil {
			net.close(socket)
		}
		testing.expectf(t, dial_error == net.Dial_Error.Refused, "connection after %d threads started and shutdown began: %v", started_threads, dial_error)

		sync.one_shot_event_signal(&startup.gate.resume)
		thread.join(server_thread)
		testing.expectf(t, startup.serve_error == nil, "serve after %d threads started: %v", started_threads, startup.serve_error)
		http.server_shutdown(&startup.server)
	}
}
