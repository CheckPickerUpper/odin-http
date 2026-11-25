#+build linux
package server_tests

import "core:mem"
import "core:sync"
import "core:thread"

Thread_Spawns :: struct #all_or_none {
	backing: mem.Allocator,
	count:   int,
}

// core:thread allocates one thread.Thread per thread it tries to create, before pthread_create runs.
count_thread_spawns :: proc(
	data: rawptr,
	mode: mem.Allocator_Mode,
	size, alignment: int,
	old_memory: rawptr,
	old_size: int,
	location := #caller_location,
) -> ([]u8, mem.Allocator_Error) {
	spawns := cast(^Thread_Spawns)data
	switch mode {
	case .Alloc, .Alloc_Non_Zeroed:
		if size == size_of(thread.Thread) {
			sync.atomic_add(&spawns.count, 1)
		}
	case .Free, .Free_All, .Resize, .Resize_Non_Zeroed, .Query_Features, .Query_Info:
	}
	return spawns.backing.procedure(spawns.backing.data, mode, size, alignment, old_memory, old_size, location)
}

// Counts the threads serve tries to spawn while this allocator is the context allocator.
@(require_results)
thread_spawns_allocator :: proc(spawns: ^Thread_Spawns) -> mem.Allocator {
	return mem.Allocator{procedure = count_thread_spawns, data = spawns}
}
