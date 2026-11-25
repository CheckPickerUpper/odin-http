package server_tests

import "base:runtime"

import "core:log"
import "core:sync"

Serving_Observer :: struct #all_or_none {
	threads:     int,
	serving:     int,
	// Signalled once `threads` server threads are serving. The test's own server proc also
	// signals it when listen or serve returns, so a server that never serves cannot hang the test.
	settled:     sync.One_Shot_Event,
	test_logger: runtime.Logger,
}

// Each server thread logs "starting event loop" once its loop is published and its accept is queued.
observe_serving :: proc(
	data: rawptr,
	level: runtime.Logger_Level,
	message: string,
	options: runtime.Logger_Options,
	location := #caller_location,
) {
	observer := cast(^Serving_Observer)data
	if message == "starting event loop" && sync.atomic_add(&observer.serving, 1) + 1 == observer.threads {
		sync.one_shot_event_signal(&observer.settled)
	}
	if level >= .Warning {
		observer.test_logger.procedure(observer.test_logger.data, level, message, options, location)
	}
}

// A logger for the server's threads that counts the threads serving and forwards warnings to the test.
@(require_results)
serving_logger :: proc(observer: ^Serving_Observer) -> log.Logger {
	observer.test_logger = context.logger
	return log.Logger{procedure = observe_serving, data = observer, lowest_level = .Debug, options = {}}
}
