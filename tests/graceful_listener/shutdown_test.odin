#+build linux
package graceful_listener

import "core:net"
import "core:strings"
import "core:sync"
import "core:testing"
import "core:thread"
import "core:time"

import http "../.."

Answer :: struct #all_or_none {
	tag:  string `json:"_tag"`,
	body: string,
}

Serving_Request :: struct #all_or_none {
	server:          http.Server,
	listening:       sync.Sema,
	handler_entered: sync.Sema,
	handler_return:  sync.Sema,
	handler_count:   int,
	endpoint:        net.Endpoint,
	listen_error:    net.Network_Error,
	serve_error:     net.Network_Error,
}

serve_request :: proc(request: ^Serving_Request) {
	opts := http.Default_Server_Opts
	opts.thread_count = 1
	request.listen_error = http.listen(&request.server, {address = net.IP4_Loopback, port = 0}, opts)
	if request.listen_error != nil {
		sync.sema_post(&request.listening)
		return
	}

	endpoint, endpoint_error := net.bound_endpoint(request.server.tcp_sock)
	assert(endpoint_error == nil)
	request.endpoint = endpoint
	sync.sema_post(&request.listening)

	request.serve_error = http.serve(&request.server, http.Handler{
		user_data = request,
		handle = proc(handler: ^http.Handler, req: ^http.Request, res: ^http.Response) {
			request := cast(^Serving_Request)handler.user_data
			request.handler_count += 1
			http.body(req, 4, res, proc(user_data: rawptr, body: http.Body, body_error: http.Body_Error) {
				res := cast(^http.Response)user_data
				assert(body_error == nil)
				response_error := http.respond_json(res, Answer{tag = "Finished", body = body})
				assert(response_error == nil)
			})
			if request.handler_count == 1 {
				sync.sema_post(&request.handler_entered)
				sync.sema_wait(&request.handler_return)
			}
		},
	})
}

@(test)
shutdown_refuses_new_connections_while_a_request_finishes :: proc(t: ^testing.T) {
	request := Serving_Request{}
	server_thread := thread.create_and_start_with_poly_data(&request, serve_request)
	defer thread.destroy(server_thread)

	sync.sema_wait(&request.listening)
	if !testing.expectf(t, request.listen_error == nil, "listen: %v", request.listen_error) {
		return
	}

	socket, dial_error := net.dial_tcp(request.endpoint)
	assert(dial_error == nil)
	defer net.close(socket)
	assert(net.set_option(socket, .Send_Timeout, time.Second * 5) == nil)
	assert(net.set_option(socket, .Receive_Timeout, time.Second * 5) == nil)

	partial_request := "POST / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 4\r\n\r\npi"
	sent, send_error := net.send_tcp(socket, transmute([]u8)partial_request)
	assert(send_error == nil && sent == len(partial_request))
	sync.sema_wait(&request.handler_entered)

	queued_socket, queued_connection_error := net.dial_tcp(request.endpoint)
	assert(queued_connection_error == nil)
	defer net.close(queued_socket)
	assert(net.set_option(queued_socket, .Send_Timeout, time.Second * 5) == nil)
	assert(net.set_option(queued_socket, .Receive_Timeout, time.Second * 5) == nil)
	queued_request := "GET /queued HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
	sent, send_error = net.send_tcp(queued_socket, transmute([]u8)queued_request)
	assert(send_error == nil && sent == len(queued_request))

	http.server_shutdown(&request.server)
	http.server_shutdown(&request.server)
	testing.expect(t, !thread.is_done(server_thread), "the incomplete request must keep the server alive")

	new_socket, new_connection_error := net.dial_tcp(request.endpoint)
	if new_connection_error == nil {
		net.close(new_socket)
	}
	testing.expectf(t, new_connection_error == net.Dial_Error.Refused,
		"new connection during shutdown: %v", new_connection_error)

	sync.sema_post(&request.handler_return)

	remaining_body := "ng"
	sent, send_error = net.send_tcp(socket, transmute([]u8)remaining_body)
	assert(send_error == nil && sent == 2)

	response := make([dynamic]u8)
	defer delete(response)
	buffer := make([]u8, 256)
	defer delete(buffer)
	for {
		received, receive_error := net.recv_tcp(socket, buffer)
		assert(receive_error == nil)
		if received == 0 { break }
		append(&response, ..buffer[:received])
	}
	testing.expectf(t, strings.has_prefix(string(response[:]), "HTTP/1.1 200 \r\n"),
		"response status: %q", string(response[:]))
	testing.expectf(t, strings.has_suffix(string(response[:]), "\r\n{\"_tag\":\"Finished\",\"body\":\"ping\"}\r\n0\r\n\r\n"),
		"completed response: %q", string(response[:]))

	received, receive_error := net.recv_tcp(queued_socket, buffer)
	testing.expectf(t, received == 0 && (receive_error == nil || receive_error == .Connection_Closed),
		"queued connection after shutdown: %d bytes, %v", received, receive_error)

	thread.join(server_thread)
	testing.expect_value(t, request.handler_count, 1)
	testing.expectf(t, request.serve_error == nil, "serve: %v", request.serve_error)
}
