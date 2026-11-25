#+build linux
package server_tests

import "base:runtime"

import "core:slice"
import "core:strings"
import "core:sync"
import "core:sys/linux"
import "core:testing"
import "core:time"

// Tests in this package read a descriptor number's state, list every thread and count every event
// loop of the process, and lower process-wide limits, so each test that starts a server holds this
// lock while it runs.
process_resources_lock: sync.Mutex

// How long a joined thread's task entry may stay in /proc/self/task before the thread counts as
// still running.
THREAD_EXIT_WAIT :: time.Second * 2

Process_Resources :: struct #all_or_none {
	task_ids:    []string,
	event_loops: int,
}

Unreadable_Resources :: struct #all_or_none {
	path:  string,
	errno: linux.Errno,
}

Resources_Read :: union #no_nil {
	Process_Resources,
	Unreadable_Resources,
}

Directory_Entries :: struct #all_or_none {
	names: [dynamic]string,
}

Directory_Read :: union #no_nil {
	Directory_Entries,
	Unreadable_Resources,
}

// Lists entry names without reading each entry, so a thread or descriptor that goes away
// during the listing cannot fail it.
@(require_results)
read_directory_entries :: proc(path: string) -> Directory_Read {
	directory, open_errno := linux.open(strings.clone_to_cstring(path, context.temp_allocator), {.DIRECTORY, .CLOEXEC})
	if open_errno != .NONE {
		return Unreadable_Resources{path = path, errno = open_errno}
	}
	defer linux.close(directory)
	entries := Directory_Entries{names = make([dynamic]string, context.temp_allocator)}
	buffer := [4096]u8{}
	for {
		written, read_errno := linux.getdents(directory, buffer[:])
		if read_errno != .NONE {
			return Unreadable_Resources{path = path, errno = read_errno}
		}
		if written == 0 {
			return entries
		}
		offset := 0
		for entry in linux.dirent_iterate_buf(buffer[:written], &offset) {
			name := linux.dirent_name(entry)
			if name != "." && name != ".." {
				append(&entries.names, strings.clone(name, context.temp_allocator))
			}
		}
	}
}

@(require_results)
count_event_loops :: proc(descriptors: Directory_Entries) -> int {
	event_loops := 0
	target := [64]u8{}
	for name in descriptors.names {
		link := strings.concatenate({"/proc/self/fd/", name}, context.temp_allocator)
		// The listing's own descriptor is closed by now and answers ENOENT.
		length, link_errno := linux.readlink(strings.clone_to_cstring(link, context.temp_allocator), target[:])
		if link_errno == .NONE && string(target[:length]) == "anon_inode:[io_uring]" {
			event_loops += 1
		}
	}
	return event_loops
}

@(require_results)
read_process_resources :: proc() -> Resources_Read {
	switch tasks in read_directory_entries("/proc/self/task") {
	case Unreadable_Resources:
		return tasks
	case Directory_Entries:
		switch descriptors in read_directory_entries("/proc/self/fd") {
		case Unreadable_Resources:
			return descriptors
		case Directory_Entries:
			return Process_Resources{task_ids = tasks.names[:], event_loops = count_event_loops(descriptors)}
		}
	}
	unreachable()
}

// The ids in `after` that `before` does not hold.
@(require_results)
find_new_task_ids :: proc(before: []string, after: []string) -> []string {
	new_task_ids := make([dynamic]string, context.temp_allocator)
	for id in after {
		if !slice.contains(before, id) {
			append(&new_task_ids, id)
		}
	}
	return new_task_ids[:]
}

// Expects every thread the test started to have exited and every event loop it acquired to be
// released. Each task listed in /proc/self/task afterwards must already be listed in `before`. Linux
// reuses a task id only once ids wrap at pid_max, so a thread the test left running has an id that
// `before` lacks, and a joined thread of an earlier test that is still listed in `before` cannot
// hide it. pthread_join returns once the kernel clears an exiting thread's id, and the kernel
// removes the thread's task entry after that, so the listing is read again until each new id is
// gone, for up to THREAD_EXIT_WAIT, and the ids still listed then fail the test.
expect_resources_released :: proc(t: ^testing.T, before: Process_Resources, loc := #caller_location) {
	start := time.tick_now()
	for {
		// Frees this read's listing at the end of each pass, and keeps `before`, which was read earlier.
		runtime.DEFAULT_TEMP_ALLOCATOR_TEMP_GUARD()
		switch after in read_process_resources() {
		case Unreadable_Resources:
			testing.expectf(t, false, "process resources afterwards: %v", after, loc = loc)
			return
		case Process_Resources:
			new_task_ids := find_new_task_ids(before.task_ids, after.task_ids)
			if len(new_task_ids) == 0 {
				testing.expect_value(t, after.event_loops, before.event_loops, loc = loc)
				return
			}
			if time.tick_since(start) >= THREAD_EXIT_WAIT {
				testing.expectf(t, false, "threads still running after %v: %v", THREAD_EXIT_WAIT, new_task_ids, loc = loc)
				return
			}
		}
		linux.sched_yield()
	}
}
