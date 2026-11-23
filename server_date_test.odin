#+test
package http

import "core:bytes"
import "core:fmt"
import "core:mem"
import "core:nbio"
import "core:net"
import "core:strings"
import "core:testing"
import "core:thread"
import "core:time"

@(private)
Date_Test_Startup :: struct #all_or_none {
	server:             ^Server,
	allocator:          mem.Allocator,
	worker_allocations: int,
	date:               [DATE_LENGTH]u8,
}

@(private)
date_test_allocate :: proc(
	allocator_data: rawptr,
	mode: mem.Allocator_Mode,
	size, alignment: int,
	old_memory: rawptr,
	old_size: int,
	location := #caller_location,
) -> ([]u8, mem.Allocator_Error) {
	startup := (^Date_Test_Startup)(allocator_data)
	// Observe the real worker allocation before serve can start accepting requests.
	if mode == .Alloc && size == size_of(Server_Thread) && location.procedure == "serve" {
		startup.worker_allocations += 1
		startup.date = startup.server.date.buf_backing
	}
	return startup.allocator.procedure(
		startup.allocator.data,
		mode,
		size,
		alignment,
		old_memory,
		old_size,
		location,
	)
}

@(private)
Date_Test_Response :: struct #all_or_none {
	server:     ^Server,
	socket:     net.TCP_Socket,
	bytes:      [dynamic]u8,
	read_error: net.TCP_Recv_Error,
}

@(private)
date_test_close_listener :: proc(server: ^Server) {
	net.close(server.tcp_sock)
	nbio.run()
	nbio.release_thread_event_loop()
}

@(test)
date_is_ready_before_workers :: proc(t: ^testing.T) {
	server := Server{}
	options := Default_Server_Opts
	options.thread_count = 1
	listen_error := listen(&server, net.Endpoint{address = net.IP4_Loopback, port = 0}, options)
	if !testing.expect(t, listen_error == nil, fmt.tprintf("listen failed: %v", listen_error)) {
		return
	}

	endpoint, endpoint_error := net.bound_endpoint(server.tcp_sock)
	if !testing.expect(t, endpoint_error == nil, fmt.tprintf("bound endpoint failed: %v", endpoint_error)) {
		date_test_close_listener(&server)
		return
	}
	socket, dial_error := net.dial_tcp(endpoint)
	if !testing.expect(t, dial_error == nil, fmt.tprintf("dial %v failed: %v", endpoint, dial_error)) {
		date_test_close_listener(&server)
		return
	}
	defer net.close(socket)
	socket_options := [?]net.Socket_Option{.Send_Timeout, .Receive_Timeout}
	for option in socket_options {
		option_error := net.set_option(socket, option, 5 * time.Second)
		if !testing.expect(t, option_error == nil, fmt.tprintf("socket option %v failed: %v", option, option_error)) {
			date_test_close_listener(&server)
			return
		}
	}

	request := bytes.Buffer{}
	defer bytes.buffer_destroy(&request)
	bytes.buffer_write_string(&request, "GET /date HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	written, send_error := net.send_tcp(socket, bytes.buffer_to_bytes(&request))
	if !testing.expect(t, send_error == nil && written == bytes.buffer_length(&request), fmt.tprintf("send failed after %d bytes: %v", written, send_error)) {
		date_test_close_listener(&server)
		return
	}

	startup := Date_Test_Startup{
		server = &server,
		allocator = server.conn_allocator,
		worker_allocations = 0,
		date = {},
	}
	server.conn_allocator = mem.Allocator{
		procedure = date_test_allocate,
		data = &startup,
	}

	response := Date_Test_Response{
		server = &server,
		socket = socket,
		bytes = make([dynamic]u8),
		read_error = nil,
	}
	defer delete(response.bytes)
	reader := thread.create_and_start_with_poly_data(&response, proc(response: ^Date_Test_Response) {
		defer server_shutdown(response.server)
		buffer := [512]u8{}
		for {
			count, read_error := net.recv_tcp(response.socket, buffer[:])
			append(&response.bytes, ..buffer[:count])
			if read_error != nil {
				response.read_error = read_error
				return
			}
			if count == 0 { return }
		}
	}, context)
	defer thread.destroy(reader)
	serve_error := serve(&server, handler(proc(_: ^Request, response: ^Response) {
		respond_plain(response, "date-ready")
	}))
	thread.join(reader)
	testing.expect(t, serve_error == nil, fmt.tprintf("serve failed: %v", serve_error))
	testing.expect(t, startup.worker_allocations == 1, fmt.tprintf("expected one worker allocation, observed %d", startup.worker_allocations))
	startup_date := string(startup.date[:])
	_, startup_parsed := date_parse(startup_date)
	testing.expect(t, startup_parsed, fmt.tprintf("Date before worker allocation is invalid: %q", startup_date))
	testing.expect(t, response.read_error == nil, fmt.tprintf("response read failed: %v", response.read_error))
	text := string(response.bytes[:])
	testing.expect(t, strings.starts_with(text, "HTTP/1.1 200 "), fmt.tprintf("unexpected response: %q", text))
	testing.expect(t, strings.ends_with(text, "date-ready"), fmt.tprintf("unexpected body: %q", text))
	date := ""
	for line in strings.split_lines_iterator(&text) {
		if line == "" { break }
		if strings.starts_with(line, "date: ") { date = line[len("date: "):] }
	}
	_, parsed := date_parse(date)
	testing.expect(t, parsed, fmt.tprintf("first HTTP Date is invalid: %q", date))
	fmt.printf("before worker allocation: Date %q, parsed %v; first HTTP response: Date %q, parsed %v\n", startup_date, startup_parsed, date, parsed)
}
