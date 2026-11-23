package body_framing

import "core:bufio"
import "core:fmt"
import "core:io"
import "core:nbio"
import "core:net"
import "core:slice"
import "core:strings"
import "core:testing"
import "core:thread"
import "core:time"

import http "../.."

@(private)
Persistent_Request :: struct #all_or_none {
	name: string,
	wire: string,
	body: string,
}

@(private)
Persistent_Client :: struct #all_or_none {
	t: ^testing.T,
	server: ^http.Server,
	endpoint: net.Endpoint,
}

@(test)
empty_bodies_keep_the_connection_for_the_next_request :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, 5 * time.Second)
	if !testing.expect_value(t, nbio.acquire_thread_event_loop(), nil) { return }
	defer nbio.release_thread_event_loop()

	server := http.Server{}
	options := http.Default_Server_Opts
	options.thread_count = 1
	if !testing.expect_value(t, http.listen(&server, {address = net.IP4_Loopback, port = 0}, options), nil) { return }
	endpoint, endpoint_error := net.bound_endpoint(server.tcp_sock)
	if !testing.expect_value(t, endpoint_error, nil) { return }
	client := Persistent_Client{t = t, server = &server, endpoint = endpoint}
	client_thread := thread.create_and_start_with_poly_data(&client, send_persistent_requests, context)
	defer thread.destroy(client_thread)
	defer thread.join(client_thread)
	testing.expect_value(t, http.serve(&server, http.handler(read_body)), nil)
}

@(private)
read_body :: proc(request: ^http.Request, response: ^http.Response) {
	http.body(request, 4, response, proc(user_data: rawptr, body: http.Body, body_error: http.Body_Error) {
		response := cast(^http.Response)user_data
		switch cause in body_error {
		case nil:
			http.respond_plain(response, fmt.tprintf("read:%s", body))
		case bufio.Scanner_Extra_Error, io.Error, http.Body_Size_Error:
			http.respond(response, http.body_error_status(cause))
		}
	})
}

@(private)
send_persistent_requests :: proc(client: ^Persistent_Client) {
	defer http.server_shutdown(client.server)
	socket, dial_error := net.dial_tcp(client.endpoint)
	if !testing.expect_value(client.t, dial_error, nil) { return }
	defer net.close(socket)
	if !testing.expect_value(client.t, net.set_option(socket, .Receive_Timeout, 2 * time.Second), nil) { return }
	if !testing.expect_value(client.t, net.set_option(socket, .Send_Timeout, 2 * time.Second), nil) { return }

	// The client package forces Connection: close, so this test sends the persistent wire directly.
	requests := []Persistent_Request{
		{name = "GET without a length", wire = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n", body = "read:"},
		{name = "POST after an undeclared length", wire = "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\nping", body = "read:ping"},
		{name = "GET with zero length", wire = "GET / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n", body = "read:"},
		{name = "POST after zero length", wire = "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\nnext", body = "read:next"},
	}
	response := strings.builder_make()
	defer strings.builder_destroy(&response)
	buffer := make([]u8, 1024)
	defer delete(buffer)
	for request in requests {
		strings.builder_reset(&response)
		sent, send_error := net.send(socket, slice.from_ptr(raw_data(request.wire), len(request.wire)))
		if !testing.expectf(client.t, send_error == nil && sent == len(request.wire), "%s: sent%d/%d: %v", request.name, sent, len(request.wire), send_error) { return }
		for {
			received, receive_error := net.recv(socket, buffer)
			if !testing.expectf(client.t, receive_error == nil && received > 0, "%s: received%d: %v", request.name, received, receive_error) { return }
			strings.write_bytes(&response, buffer[:received])
			wire_response := strings.to_string(response)
			separator := strings.index(wire_response, "\r\n\r\n")
			if separator >= 0 && len(wire_response) >= separator + 4 + len(request.body) {
				if !testing.expectf(client.t, strings.has_prefix(wire_response, "HTTP/1.1 200 "), "%s: response%s", request.name, wire_response) { return }
				if !testing.expect_value(client.t, wire_response[separator + 4:], request.body) { return }
				break
			}
		}
	}
}
