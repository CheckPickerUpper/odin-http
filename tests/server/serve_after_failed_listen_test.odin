#+build linux
package server_tests

import "core:nbio"
import "core:net"
import "core:sync"
import "core:sys/linux"
import "core:testing"

import http "../.."

// The caller holds its own reference to this thread's event loop, so a serve that released it would
// take the caller's. A failed listen stores no socket, so the descriptor number in `tcp_sock` belongs
// to someone else, and a serve that closed it would close theirs.
serve_after_failed_listen :: proc(t: ^testing.T, caller_loop: ^nbio.Event_Loop) {
	server := http.Server{}
	// 192.0.2.1 is in TEST-NET-1, which no interface has, so binding to it fails.
	listen_error := http.listen(&server, {address = net.IP4_Address{192, 0, 2, 1}, port = 0})
	if !testing.expectf(t, listen_error != nil, "listen on an address no interface has succeeded") {
		http.server_shutdown(&server)
		_ = http.serve(&server, http.handler(proc(_: ^http.Request, _: ^http.Response) {}))
		return
	}
	_, descriptor_before := linux.fcntl(linux.Fd(server.tcp_sock), linux.F_GETFD)

	serve_error := http.serve(&server, http.handler(proc(_: ^http.Request, _: ^http.Response) {
		panic("a server whose listen failed cannot serve")
	}))
	_, descriptor_after := linux.fcntl(linux.Fd(server.tcp_sock), linux.F_GETFD)

	testing.expect_value(t, serve_error, nil)
	testing.expect_value(t, descriptor_after, descriptor_before)
	testing.expect_value(t, nbio.current_thread_event_loop(), caller_loop)
}

@(test)
serve_after_a_failed_listen_releases_and_closes_nothing :: proc(t: ^testing.T) {
	sync.mutex_lock(&process_resources_lock)
	defer sync.mutex_unlock(&process_resources_lock)
	if acquire_error := nbio.acquire_thread_event_loop(); acquire_error != .None {
		testing.expectf(t, false, "the caller's event loop: %v", acquire_error)
		return
	}
	defer nbio.release_thread_event_loop()
	serve_after_failed_listen(t, nbio.current_thread_event_loop())
}
