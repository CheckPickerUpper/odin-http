package http

import "base:runtime"

import "core:bufio"
import "core:bytes"
import "core:c/libc"
import "core:fmt"
import "core:log"
import "core:mem"
import "core:mem/virtual"
import "core:nbio"
import "core:net"
import "core:os"
import "core:slice"
import "core:sync"
import "core:thread"
import "core:time"

Server_Opts :: struct {
	// Whether the server should accept every request that sends a "Expect: 100-continue" header automatically.
	// Defaults to true.
	auto_expect_continue:    bool,
	// When this is true, any HEAD request is automatically redirected to the handler as a GET request.
	// Then, when the response is sent, the body is removed from the response.
	// Defaults to true.
	redirect_head_to_get:    bool,
	// Limit the maximum number of bytes to read for the request line (first line of request containing the URI).
	// The HTTP spec does not specify any limits but in practice it is safer.
	// RFC 7230 3.1.1 says:
	// Various ad hoc limitations on request-line length are found in
	// practice.  It is RECOMMENDED that all HTTP senders and recipients
	// support, at a minimum, request-line lengths of 8000 octets.
	// defaults to 8000.
	limit_request_line:      int,
	// Limit the length of the headers.
	// The HTTP spec does not specify any limits but in practice it is safer.
	// defaults to 8000.
	limit_headers:           int,
	// The thread count to use, defaults to your core count.
	thread_count:            int,
	// Called on the retiring thread before it releases the event loop. Receives the handler's
	// user data and the tick failure, or .None after the loop drains during shutdown.
	on_event_loop_retire:    proc(user_data: rawptr, cause: nbio.General_Error),

	// // The initial size of the temp_allocator for each connection, defaults to 256KiB and doubles
	// // each time it needs to grow.
	// // NOTE: this value is assigned globally, running multiple servers with a different value will
	// // not work.
	// initial_temp_block_cap:  uint,
	// // The amount of free blocks each thread is allowed to hold on to before deallocating excess.
	// // Defaults to 64.
	// max_free_blocks_queued:  uint,
}

Default_Server_Opts := Server_Opts {
	auto_expect_continue    = true,
	redirect_head_to_get    = true,
	limit_request_line      = 8000,
	limit_headers           = 8000,
	// initial_temp_block_cap  = 256 * mem.Kilobyte,
	// max_free_blocks_queued  = 64,
}

Server_State :: enum {
	Uninitialized,
	Idle,
	Listening,
	Serving,
	Running,
	Closing,
	Cleaning,
	Closed,
}

@(private)
Server_Listener_State :: enum u8 {
	Unbound,
	Listening,
	Draining,
	Closed,
}

@(private)
Server_Wake_State :: enum u8 {
	Unpublished,
	Published,
	Notifying,
	Retired,
}

@(private)
Server_Startup :: enum {
	// Every server thread was spawned and acquired its event loop.
	Serving,
	// A server thread could not be spawned or could not acquire its event loop. The threads that
	// acquired theirs release them without serving.
	Abandoned,
}

Server :: struct {
	opts:           Server_Opts,
	tcp_sock:       net.TCP_Socket,
	conn_allocator: mem.Allocator,
	handler:        Handler,

	threads:        []Server_Thread,
	// Threads that publish their loop after shutdown starts observe closing before
	// blocking, even when the notifier could not claim their unpublished loop.
	closing:        Atomic(bool),
	_listener:      Atomic(Server_Listener_State),
	_shutdown_wake: Atomic(Server_Wake_State),
	// Claimed loops and the thread slice stay live until notification finishes.
	_shutdown_notified: sync.One_Shot_Event,
	// Signalled by the server_shutdown that drains the listener once it no longer uses the socket.
	_listener_drained: sync.One_Shot_Event,
	// Threads will decrement the wait group when they have fully closed/shutdown.
	// The main thread waits on this to clean up global data and return.
	threads_closed: sync.Wait_Group,
	// Signalled once serve stopped spawning threads; `startup` then says whether they serve.
	startup_done:   sync.One_Shot_Event,
	startup:        Server_Startup,

	// Updated every second with an updated date, this speeds up the server considerably
	// because it would otherwise need to call time.now() and format the date on each response.
	date:           Server_Date,
}

Server_Thread :: struct {
	thread:     ^thread.Thread,
	event_loop: ^nbio.Event_Loop,
	conns:      map[net.TCP_Socket]^Connection,
	state:      Server_State,
	accept:     ^nbio.Operation,
	// Signalled by a spawned thread once it acquired its event loop or stored why it could not in `loop_error`.
	reported:   sync.One_Shot_Event,
	loop_error: nbio.General_Error,

	// free_temp_blocks:       map[int]queue.Queue(^Block),
	// free_temp_blocks_count: int,
}

@(private, disabled = ODIN_DISABLE_ASSERT)
assert_has_td :: #force_inline proc(loc := #caller_location) {
	assert(td.state != .Uninitialized, "The thread you are calling from is not a server/handler thread", loc)
}

@(thread_local)
td: ^Server_Thread

Default_Endpoint := net.Endpoint {
	address = net.IP4_Any,
	port    = 8080,
}

Server_Thread_Error :: enum {
	None,
	// Creating a server thread failed. serve joins the threads it spawned, none of which served,
	// and releases what listen acquired before returning.
	Spawn_Failed,
}

// The errors listen and serve return. A `nbio.General_Error` is a failed event loop acquire: of the
// calling thread in listen, or of a server thread in serve. serve then joins the threads it spawned,
// none of which served, and releases every event loop reference they and listen acquired.
Server_Error :: union #shared_nil {
	net.Network_Error,
	Server_Thread_Error,
	nbio.General_Error,
}

listen :: proc(
	s: ^Server,
	endpoint: net.Endpoint = Default_Endpoint,
	opts: Server_Opts = Default_Server_Opts,
) -> Server_Error {
	if acquire_err := nbio.acquire_thread_event_loop(); acquire_err != .None {
		server_shutdown(s)
		return acquire_err
	}

	socket, listen_error := nbio.listen_tcp(endpoint)
	if listen_error != nil {
		nbio.run()
		nbio.release_thread_event_loop()
		server_shutdown(s)
		return listen_error
	}
	_server_listener_init(s, socket, opts)
	return nil
}

// The server owns the socket and one reference to its creator thread's event loop.
Listening_On_Socket :: struct #all_or_none {}

// The caller still owns the socket when its event loop cannot be acquired.
Listener_Event_Loop_Unavailable :: struct #all_or_none {
	socket: net.TCP_Socket,
	error:  nbio.General_Error,
}

Listen_On_Socket_Outcome :: union #no_nil {
	Listening_On_Socket,
	Listener_Event_Loop_Unavailable,
}

// Takes ownership of an already bound, listening, nonblocking TCP socket created through nbio.
// Keep the creation loop acquired until this call returns. The caller configures
// the socket and retains ownership on failure. Call serve
// on this same thread, including after server_shutdown, to close the socket and
// release the one event-loop reference this call acquires.
@(require_results)
listen_on_socket :: proc(
	s: ^Server,
	socket: net.TCP_Socket,
	opts: Server_Opts = Default_Server_Opts,
) -> Listen_On_Socket_Outcome {
	acquire_error := nbio.acquire_thread_event_loop()
	// nbio also carries native errno values through General_Error.
	if acquire_error != .None {
		return Listener_Event_Loop_Unavailable{socket = socket, error = acquire_error}
	}
	_server_listener_init(s, socket, opts)
	return Listening_On_Socket{}
}

@(private)
_server_listener_init :: proc(s: ^Server, socket: net.TCP_Socket, opts: Server_Opts) {
	s.opts = opts
	s.tcp_sock = socket
	s.conn_allocator = context.allocator
	atomic_store(&s._listener, .Listening)
}

serve :: proc(s: ^Server, h: Handler) -> (err: Server_Error) {
	if atomic_load(&s.closing) {
		_server_listen_release(s)
		atomic_store(&s._shutdown_wake, .Retired)
		return
	}
	s.handler = h

	if s.opts.thread_count == 0 {
		s.opts.thread_count = os.get_processor_core_count()
	}

	thread_count := max(1, s.opts.thread_count)
	s.threads = make([]Server_Thread, thread_count, s.conn_allocator)
	atomic_store(&s._shutdown_wake, .Published)

	// Each thread acquires its event loop before serve spawns the next one, so the first failure
	// stops spawning and serve returns that thread's error.
	spawned := 0
	startup := Server_Startup.Serving
	for &td in s.threads[1:] {
		td.thread = thread.create_and_start_with_poly_data2(s, &td, _server_thread_start, context)
		if td.thread == nil {
			err = Server_Thread_Error.Spawn_Failed
			startup = .Abandoned
			break
		}
		spawned += 1
		sync.one_shot_event_wait(&td.reported)
		if td.loop_error != .None {
			err = td.loop_error
			startup = .Abandoned
			break
		}
	}

	s.startup = startup
	switch startup {
	case .Serving:
		sync.wait_group_add(&s.threads_closed, thread_count)
		// Start keeping track of and caching the date for the required date header, before any
		// thread serves.
		server_date_start(s)
		sync.one_shot_event_signal(&s.startup_done)

		_server_thread_init(s, &s.threads[0])

		sync.wait(&s.threads_closed)

		log.debug("server threads are done, shutting down")

		_server_wake_retire(s)
		_ = _server_listener_close(s)
	case .Abandoned:
		// A later serve call returns at once, as it does after a failed listen.
		server_shutdown(s)
		sync.one_shot_event_signal(&s.startup_done)
		switch atomic_load(&s._listener) {
		case .Draining:
			// The server_shutdown that drains the listener, this one or one that began during
			// startup, may still be shutting the socket down.
			sync.one_shot_event_wait(&s._listener_drained)
		case .Unbound, .Listening, .Closed:
		}
		_server_wake_retire(s)
		_server_listen_release(s)
	}

	for t in s.threads[1:][:spawned] { thread.destroy(t.thread) }
	delete(s.threads, s.conn_allocator)

	return
}

// Ends shutdown wake publication, after a server_shutdown that claimed the threads' loops has
// finished waking them.
@(private)
_server_wake_retire :: proc(s: ^Server) {
	switch sync.atomic_exchange(&s._shutdown_wake.raw, Server_Wake_State.Retired) {
	case .Unpublished, .Published, .Retired:
	case .Notifying:
		sync.one_shot_event_wait(&s._shutdown_notified)
	}
}

// Closes the socket listen opened and releases the event loop reference listen acquired,
// for a serve that returns without serving.
@(private)
_server_listen_release :: proc(s: ^Server) {
	switch _server_listener_close(s) {
	case .Unbound, .Closed:
	case .Listening, .Draining:
		nbio.run()
		nbio.release_thread_event_loop()
	}
}

listen_and_serve :: proc(
	s: ^Server,
	h: Handler,
	endpoint: net.Endpoint = Default_Endpoint,
	opts: Server_Opts = Default_Server_Opts,
) -> (err: Server_Error) {
	listen(s, endpoint, opts) or_return
	return serve(s, h)
}

// Runs on each spawned server thread: acquires the thread's event loop, reports the result to serve,
// and serves once every other thread acquired its loop.
@(private)
_server_thread_start :: proc(s: ^Server, ttd: ^Server_Thread) {
	if acquire_err := nbio.acquire_thread_event_loop(); acquire_err != .None {
		ttd.loop_error = acquire_err
		sync.one_shot_event_signal(&ttd.reported)
		return
	}
	sync.one_shot_event_signal(&ttd.reported)

	sync.one_shot_event_wait(&s.startup_done)
	switch s.startup {
	case .Serving:
		_server_thread_init(s, ttd)
	case .Abandoned:
		// This thread never serves, so the release in _server_thread_init's defer never runs for it.
		nbio.release_thread_event_loop()
	}
}

_server_thread_init :: proc(s: ^Server, ttd: ^Server_Thread) {
	td = ttd
	defer sync.wait_group_done(&s.threads_closed)

	td.conns = make(map[net.TCP_Socket]^Connection)
	// td.free_temp_blocks = make(map[int]queue.Queue(^Block))

	sync.atomic_store(&td.event_loop, nbio.current_thread_event_loop())
	defer {
		// A missing loop at its owner's sole retirement means the notifier claimed
		// it. Wait until every claimed wake finishes before releasing the loop.
		if sync.atomic_exchange(&ttd.event_loop, nil) == nil {
			sync.one_shot_event_wait(&s._shutdown_notified)
		}
		nbio.release_thread_event_loop()
		if ttd != &s.threads[0] {
			runtime.default_temp_allocator_destroy(auto_cast context.temp_allocator.data)
		}
	}

	log.debug("accepting connections")

	_server_thread_accept(s)

	log.debug("starting event loop")
	td.state = .Serving
	retirement_cause: nbio.General_Error
	for {
		if atomic_load(&s.closing) {
			retirement_cause = _server_thread_shutdown(s)
			break
		}
		retirement_cause = nbio.tick()
		if retirement_cause != .None { break }
	}

	if notify := s.opts.on_event_loop_retire; notify != nil {
		notify(s.handler.user_data, retirement_cause)
	} else if td.state == .Closing {
		fmt.assertf(retirement_cause == .None, "IO tick error during shutdown: %v", retirement_cause)
	}
	if td.state != .Closed && retirement_cause != .None {
		log.errorf("non-blocking io tick error: %v", retirement_cause)
	}
	log.debug("event loop end")
}


// The time between checks and closes of connections in a graceful shutdown.
@(private)
SHUTDOWN_INTERVAL :: time.Millisecond * 100

// Starts a graceful shutdown.
//
// Serving threads finish active requests, close idle connections, and free the
// remaining server resources before serve returns. On Linux, the listener stops
// accepting connections before draining active requests.
server_shutdown :: proc(s: ^Server) {
	when ODIN_OS == .Linux {
		listener, _ := sync.atomic_compare_exchange_strong(&s._listener.raw, .Listening, .Draining)
		switch listener {
		case .Unbound:
			atomic_store(&s.closing, true)
			return
		case .Draining, .Closed:
			return
		case .Listening:
		}

		net.shutdown(s.tcp_sock, .Both)
		sync.one_shot_event_signal(&s._listener_drained)
		// Serving threads can free the descriptor once closing is published.
	}
	atomic_store(&s.closing, true)
	publication, _ := sync.atomic_compare_exchange_strong(&s._shutdown_wake.raw, .Published, .Notifying)
	switch publication {
	case .Unpublished, .Notifying, .Retired:
		return
	case .Published:
	}
	for &t in s.threads {
		if event_loop := sync.atomic_exchange(&t.event_loop, nil); event_loop != nil {
			nbio.wake_up(event_loop)
		}
	}
	sync.one_shot_event_signal(&s._shutdown_notified)
}

@(private, require_results)
_server_listener_close :: proc(s: ^Server) -> Server_Listener_State {
	previous := sync.atomic_exchange(&s._listener.raw, Server_Listener_State.Closed)
	switch previous {
	case .Unbound, .Closed:
		return previous
	case .Listening:
		net.shutdown(s.tcp_sock, .Both)
	case .Draining:
	}
	net.close(s.tcp_sock)
	return previous
}

_server_thread_shutdown :: proc(s: ^Server, loc := #caller_location) -> nbio.General_Error {
	assert_has_td(loc)

	td.state = .Closing
	defer delete(td.conns)
	// defer {
	// 	blocks: int
	// 	for _, &bucket in td.free_temp_blocks {
	// 		for block in queue.pop_front_safe(&bucket) {
	// 			blocks += 1
	// 			free(block)
	// 		}
	// 		queue.destroy(&bucket)
	// 	}
	// 	delete(td.free_temp_blocks)
	// 	log.infof("had %i temp blocks to spare", blocks)
	// }

	for {
		for sock, conn in td.conns {
			#partial switch conn.state {
			case .Active:
				log.infof("shutdown: connection %i still active", sock)
			case .New, .Idle, .Pending:
				log.infof("shutdown: closing connection %i", sock)
				connection_close(conn)
			case .Closing:
				log.debugf("shutdown: connection %i is closing", sock)
			case .Closed:
				log.warn("closed connection in connections map, maybe a race or logic error")
			}
		}

		when ODIN_OS == .Linux {
			if len(td.conns) == 0 && td.accept == nil {
				break
			}
		} else {
			if len(td.conns) == 0 {
				break
			}
		}

		if cause := nbio.tick(); cause != .None {
			return cause
		}
	}

	td.state = .Cleaning

	when ODIN_OS != .Linux {
		nbio.remove(td.accept)
		td.accept = nil
	}

	cause := nbio.run()

	td.state = .Closed

	if cause == .None || s.opts.on_event_loop_retire == nil { log.info("shutdown: done") }
	return cause
}

@(private)
on_interrupt_server: ^Server
@(private)
on_interrupt_context: runtime.Context

// Registers a signal handler to shutdown the server gracefully on interrupt signal.
// Can only be called once in the lifetime of the program because of a hacky interaction with libc.
server_shutdown_on_interrupt :: proc(s: ^Server) {
	on_interrupt_server = s
	on_interrupt_context = context

	libc.signal(
		libc.SIGINT,
		proc "cdecl" (_: i32) {
			context = on_interrupt_context

			// Force close on second signal.
			if atomic_load(&on_interrupt_server.closing) {
				os.exit(1)
			}

			server_shutdown(on_interrupt_server)
		},
	)
}

// Taken from Go's implementation,
// The maximum amount of bytes we will read (if handler did not)
// in order to get the connection ready for the next request.
@(private)
Max_Post_Handler_Discard_Bytes :: 256 << 10

// How long to wait before actually closing a connection.
// This is to make sure the client can fully receive the response.
@(private)
Conn_Close_Delay :: time.Millisecond * 500

Connection_State :: enum {
	Pending, // Pending a client to attach.
	New, // Got client, waiting to service first request.
	Active, // Servicing request.
	Idle, // Waiting for next request.
	Will_Close, // Closing after the current response is sent.
	Closing, // Going to close, cleaning up.
	Closed, // Fully closed.
}

@(private)
connection_set_state :: proc(c: ^Connection, s: Connection_State) -> bool {
	if s < .Closing && c.state >= .Closing {
		return false
	}

	if s == .Closing && c.state == .Closed {
		return false
	}

	c.state = s
	return true
}

// TODO/PERF: pool the connections, saves having to allocate scanner buf and temp_allocator every time.
Connection :: struct {
	server:         ^Server,
	socket:         net.TCP_Socket,
	state:          Connection_State,
	scanner:        Scanner,
	temp_allocator: virtual.Arena,
	loop:           Loop,
}

// Loop/request cycle state.
@(private)
Loop :: struct {
	conn: ^Connection,
	req:  Request,
	res:  Response,
}

@(private)
connection_close :: proc(c: ^Connection, loc := #caller_location) {
	assert_has_td(loc)

	if c.state >= .Closing {
		log.infof("connection %i already closing/closed", c.socket)
		return
	}

	log.debugf("closing connection: %i", c.socket)

	c.state = .Closing

	// RFC 7230 6.6.

	// Close read side of the connection, then wait a little bit, allowing the client
	// to process the closing and receive any remaining data.
	net.shutdown(c.socket, net.Shutdown_Manner.Send)

	nbio.timeout_poly(Conn_Close_Delay, c, proc(_: ^nbio.Operation, c: ^Connection) {
		nbio.close_poly(c.socket, c, proc(_: ^nbio.Operation, c: ^Connection) {
			log.debugf("closed connection: %i", c.socket)

			c.state = .Closed

			// allocator_destroy(&c.temp_allocator)
			virtual.arena_destroy(&c.temp_allocator)

			scanner_destroy(&c.scanner)
			delete_key(&td.conns, c.socket)
			free(c, c.server.conn_allocator)
		})
	})
}

@(private)
_server_thread_accept :: proc(server: ^Server) {
	when ODIN_OS == .Linux {
		switch atomic_load(&server._listener) {
		case .Unbound, .Draining, .Closed:
			return
		case .Listening:
		}
	}
	td.accept = nbio.accept_poly(server.tcp_sock, server, on_accept)
}

@(private)
on_accept :: proc(op: ^nbio.Operation, server: ^Server) {
	td.accept = nil

	when ODIN_OS == .Linux {
		switch atomic_load(&server._listener) {
		case .Unbound, .Draining, .Closed:
			if op.accept.err == nil {
				net.close(op.accept.client)
			}
			return
		case .Listening:
		}
	}

	if op.accept.err != nil {
		#partial switch op.accept.err {
		case .Insufficient_Resources:
			log.error("Connection limit reached, trying again in a bit")
			nbio.timeout_poly(time.Second, server, proc(_: ^nbio.Operation, server: ^Server) {
				_server_thread_accept(server)
			})
			return
		}

		fmt.panicf("accept error: %v", op.accept.err)
	}

	// Accept next connection.
	_server_thread_accept(server)

	c := new(Connection, server.conn_allocator)
	c.state = .New
	c.server = server
	c.socket = op.accept.client
	c.loop.req.client = op.accept.client_endpoint

	td.conns[c.socket] = c

	log.debugf("new connection with thread, got %d conns", len(td.conns))
	conn_handle_reqs(c)
}

@(private)
conn_handle_reqs :: proc(c: ^Connection) {
	// TODO/PERF: not sure why this is allocated on the connections allocator, can't it use the arena?
	scanner_init(&c.scanner, c, c.server.conn_allocator)

	// allocator_init(&c.temp_allocator, c.server.conn_allocator)
	// context.temp_allocator = allocator(&c.temp_allocator)
	err := virtual.arena_init_growing(&c.temp_allocator)
	assert(err == nil)
	context.temp_allocator = virtual.arena_allocator(&c.temp_allocator)

	conn_handle_req(c, context.temp_allocator)
}

@(private)
conn_handle_req :: proc(c: ^Connection, allocator := context.temp_allocator) {
	on_rline1 :: proc(loop: rawptr, token: string, err: bufio.Scanner_Error) {
		l := cast(^Loop)loop

		if !connection_set_state(l.conn, .Active) { return }

		if err != nil {
			if err == .EOF {
				log.debugf("client disconnected (EOF)")
			} else {
				log.warnf("request scanner error: %v", err)
			}

			clean_request_loop(l.conn, close = true)
			return
		}

		// In the interest of robustness, a server that is expecting to receive
		// and parse a request-line SHOULD ignore at least one empty line (CRLF)
		// received prior to the request-line.
		if len(token) == 0 {
			log.debug("first request line empty, skipping in interest of robustness")
			scanner_scan(&l.conn.scanner, loop, on_rline2)
			return
		}

		on_rline2(loop, token, err)
	}

	on_rline2 :: proc(loop: rawptr, token: string, err: bufio.Scanner_Error) {
		l := cast(^Loop)loop

		if err != nil {
			log.warnf("request scanning error: %v", err)
			clean_request_loop(l.conn, close = true)
			return
		}

		rline, err := requestline_parse(token, context.temp_allocator)
		switch err {
		case .Method_Not_Implemented:
			log.infof("request-line %q invalid method", token)
			headers_set_close(&l.res.headers)
			l.res.status = .Not_Implemented
			respond(&l.res)
			return
		case .Invalid_Version_Format, .Not_Enough_Fields:
			log.warnf("request-line %q invalid: %s", token, err)
			clean_request_loop(l.conn, close = true)
			return
		case .None:
			l.req.line = rline
		}

		// Might need to support more versions later.
		if rline.version.major != 1 || rline.version.minor > 1 {
			log.infof("request http version not supported %v", rline.version)
			headers_set_close(&l.res.headers)
			l.res.status = .HTTP_Version_Not_Supported
			respond(&l.res)
			return
		}

		l.req.url = url_parse(rline.target.(string))

		l.conn.scanner.max_token_size = l.conn.server.opts.limit_headers
		scanner_scan(&l.conn.scanner, loop, on_header_line)
	}

	on_header_line :: proc(loop: rawptr, token: string, err: bufio.Scanner_Error) {
		l := cast(^Loop)loop

		if err != nil {
			log.warnf("request scanning error: %v", err)
			clean_request_loop(l.conn, close = true)
			return
		}

		// The first empty line denotes the end of the headers section.
		if len(token) == 0 {
			on_headers_end(l)
			return
		}

		if _, ok := header_parse(&l.req.headers, token); !ok {
			log.warnf("header-line %s is invalid", token)
			headers_set_close(&l.res.headers)
			l.res.status = .Bad_Request
			respond(&l.res)
			return
		}

		l.conn.scanner.max_token_size -= len(token)
		if l.conn.scanner.max_token_size <= 0 {
			log.warn("request headers too large")
			headers_set_close(&l.res.headers)
			l.res.status = .Request_Header_Fields_Too_Large
			respond(&l.res)
			return
		}

		scanner_scan(&l.conn.scanner, loop, on_header_line)
	}

	on_headers_end :: proc(l: ^Loop) {
		if !headers_validate_for_server(&l.req.headers) {
			log.warn("request headers are invalid")
			headers_set_close(&l.res.headers)
			l.res.status = .Bad_Request
			respond(&l.res)
			return
		}

		l.req.headers.readonly = true

		l.conn.scanner.max_token_size = bufio.DEFAULT_MAX_SCAN_TOKEN_SIZE

		// Automatically respond with a continue status when the client has the Expect: 100-continue header.
		if expect, ok := headers_get_unsafe(l.req.headers, "expect");
		   ok && expect == "100-continue" && l.conn.server.opts.auto_expect_continue {

			l.res.status = .Continue

			respond(&l.res)
			return
		}

		rline := &l.req.line.(Requestline)
		// An options request with the "*" is a no-op/ping request to
		// check for server capabilities and should not be sent to handlers.
		if rline.method == .Options && rline.target.(string) == "*" {
			l.res.status = .OK
			respond(&l.res)
		} else {
			// Give the handler this request as a GET, since the HTTP spec
			// says a HEAD is identical to a GET but just without writing the body,
			// handlers shouldn't have to worry about it.
			is_head := rline.method == .Head
			if is_head && l.conn.server.opts.redirect_head_to_get {
				l.req.is_head = true
				rline.method = .Get
			}

			l.conn.server.handler.handle(&l.conn.server.handler, &l.req, &l.res)
		}
	}

	c.loop.conn = c
	c.loop.res._conn = c
	c.loop.req._scanner = &c.scanner
	request_init(&c.loop.req, allocator)
	response_init(&c.loop.res, allocator)

	c.scanner.max_token_size = c.server.opts.limit_request_line
	scanner_scan(&c.scanner, &c.loop, on_rline1)
}

// A buffer that will contain the date header for the current second.
@(private)
Server_Date :: struct {
	buf_backing: [DATE_LENGTH]byte,
	buf:         bytes.Buffer,
}

@(private)
server_date_start :: proc(s: ^Server) {
	s.date.buf.buf = slice.into_dynamic(s.date.buf_backing[:])
	server_date_update(nil, s)
}

// Updates the time and schedules itself for after a second.
@(private)
server_date_update :: proc(_: ^nbio.Operation, s: ^Server) {
	if atomic_load(&s.closing) { return }

	nbio.timeout_poly(time.Second, s, server_date_update)

	bytes.buffer_reset(&s.date.buf)
	date_write(bytes.buffer_to_stream(&s.date.buf), time.now())
}

@(private)
server_date :: proc(s: ^Server) -> string {
	return string(s.date.buf_backing[:])
}
