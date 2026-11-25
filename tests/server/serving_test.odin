#+build linux
package server_tests

import "core:net"
import "core:strings"
import "core:sync"
import "core:testing"
import "core:thread"
import "core:time"

import http "../.."

Server_Run :: struct #all_or_none {
	server:         http.Server,
	observer:       Serving_Observer,
	returned:       Maybe(Server_Return),
	endpoint:       net.Endpoint,
	endpoint_error: net.Socket_Info_Error,
}

serve_on_loopback :: proc(run: ^Server_Run) {
	context.logger = serving_logger(&run.observer)
	defer sync.one_shot_event_signal(&run.observer.settled)

	opts := http.Default_Server_Opts
	opts.thread_count = run.observer.threads
	if listen_error := http.listen(&run.server, {address = net.IP4_Loopback, port = 0}, opts); listen_error != nil {
		run.returned = Listen_Failed{error = listen_error}
		return
	}
	run.endpoint, run.endpoint_error = net.bound_endpoint(run.server.tcp_sock)

	serve_error := http.serve(&run.server, http.handler(proc(_: ^http.Request, res: ^http.Response) {
		http.respond_plain(res, "served")
	}))
	run.returned = Serve_Returned{error = serve_error}
}

Response_Received :: struct #all_or_none {
	status_line: string,
}

Request_Step :: enum {
	Dial,
	Set_Deadline,
	Send,
	Receive,
}

No_Response :: struct #all_or_none {
	step:  Request_Step,
	error: net.Network_Error,
}

Root_Response :: union #no_nil {
	Response_Received,
	No_Response,
}

@(require_results)
request_root :: proc(endpoint: net.Endpoint) -> Root_Response {
	socket, dial_error := net.dial_tcp(endpoint)
	if dial_error != nil {
		return No_Response{step = .Dial, error = dial_error}
	}
	defer net.close(socket)
	// The deadline bounds a server that accepted the connection and never answered.
	if option_error := net.set_option(socket, .Receive_Timeout, time.Second * 5); option_error != nil {
		return No_Response{step = .Set_Deadline, error = option_error}
	}

	request := "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
	if _, send_error := net.send_tcp(socket, transmute([]u8)request); send_error != nil {
		return No_Response{step = .Send, error = send_error}
	}

	response := strings.builder_make(context.temp_allocator)
	buffer := [1024]u8{}
	for {
		received, recv_error := net.recv_tcp(socket, buffer[:])
		if recv_error != nil {
			return No_Response{step = .Receive, error = recv_error}
		}
		if received == 0 {
			break
		}
		strings.write_bytes(&response, buffer[:received])
	}
	status_line, _, _ := strings.partition(strings.to_string(response), "\r\n")
	return Response_Received{status_line = status_line}
}

@(test)
a_request_is_answered_once_every_server_thread_serves :: proc(t: ^testing.T) {
	// A server thread that never starts serving leaves `settled` unsignalled until this timeout.
	testing.set_fail_timeout(t, time.Second * 10)
	sync.mutex_lock(&process_resources_lock)
	defer sync.mutex_unlock(&process_resources_lock)
	defer free_all(context.temp_allocator)

	run := new(Server_Run)
	defer free(run)
	run.observer.threads = 4
	server_thread := thread.create_and_start_with_poly_data(run, serve_on_loopback)
	defer thread.destroy(server_thread)

	sync.one_shot_event_wait(&run.observer.settled)
	switch returned in run.returned {
	case nil:
		if !testing.expectf(t, run.endpoint_error == nil, "bound endpoint: %v", run.endpoint_error) {
			http.server_shutdown(&run.server)
			return
		}
		response := request_root(run.endpoint)
		http.server_shutdown(&run.server)
		thread.join(server_thread)
		switch received in response {
		case Response_Received:
			testing.expect_value(t, received.status_line, "HTTP/1.1 200 ")
		case No_Response:
			testing.expectf(t, false, "every thread serves and the request was not answered: %v", received)
		}
		testing.expectf(t, run.returned == Server_Return(Serve_Returned{error = nil}), "serve served, then returned %v", run.returned)
	case Server_Return:
		testing.expectf(t, false, "the server returned before every thread served: %v", returned)
	}
}
