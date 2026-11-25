#+build linux
package server_tests

import "core:sys/linux"

// While a test runs restricted, every descriptor number below this limit is taken except the ones it leaves free.
DESCRIPTOR_LIMIT :: 256

Filler_Step :: enum {
	Open_Filler,
	Too_Few_Free,
}

Fillers_Not_Opened :: struct #all_or_none {
	step:  Filler_Step,
	errno: linux.Errno,
}

Descriptors_Restricted :: struct #all_or_none {
	limit:   Limit_Lowered,
	fillers: [dynamic]linux.Fd,
}

Restriction_Failed :: union #no_nil {
	Limit_Not_Lowered,
	Fillers_Not_Opened,
}

Descriptor_Restriction :: union #no_nil {
	Descriptors_Restricted,
	Restriction_Failed,
}

// Lowers the soft descriptor limit and takes every free number below it, leaving `left_free` numbers.
@(require_results)
restrict_descriptors :: proc(left_free: int) -> Descriptor_Restriction {
	switch lowering in lower_soft_limit(.NOFILE, DESCRIPTOR_LIMIT) {
	case Limit_Not_Lowered:
		return Restriction_Failed(lowering)
	case Limit_Lowered:
		restricted := Descriptors_Restricted{limit = lowering, fillers = make([dynamic]linux.Fd)}
		for {
			filler, open_errno := linux.open("/dev/null", {.CLOEXEC})
			if open_errno == .EMFILE {
				break
			}
			if open_errno != .NONE {
				release_descriptors(restricted)
				return Restriction_Failed(Fillers_Not_Opened{step = .Open_Filler, errno = open_errno})
			}
			append(&restricted.fillers, filler)
		}
		if len(restricted.fillers) < left_free {
			release_descriptors(restricted)
			return Restriction_Failed(Fillers_Not_Opened{step = .Too_Few_Free, errno = .EMFILE})
		}
		for filler in restricted.fillers[len(restricted.fillers) - left_free:] {
			linux.close(filler)
		}
		resize(&restricted.fillers, len(restricted.fillers) - left_free)
		return restricted
	}
	unreachable()
}

release_descriptors :: proc(restricted: Descriptors_Restricted) {
	for filler in restricted.fillers {
		linux.close(filler)
	}
	delete(restricted.fillers)
	restore_limit(restricted.limit)
}
