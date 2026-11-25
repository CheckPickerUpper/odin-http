#+build linux
package server_tests

import "core:sys/linux"

Limit_Step :: enum {
	Read_Limit,
	Lower_Limit,
}

Limit_Not_Lowered :: struct #all_or_none {
	kind:  linux.RLimit_Kind,
	step:  Limit_Step,
	errno: linux.Errno,
}

Limit_Lowered :: struct #all_or_none {
	kind:  linux.RLimit_Kind,
	saved: linux.RLimit,
}

Limit_Lowering :: union #no_nil {
	Limit_Lowered,
	Limit_Not_Lowered,
}

@(require_results)
lower_soft_limit :: proc(kind: linux.RLimit_Kind, soft: uint) -> Limit_Lowering {
	saved := linux.RLimit{cur = 0, max = 0}
	if errno := linux.getrlimit(kind, &saved); errno != .NONE {
		return Limit_Not_Lowered{kind = kind, step = .Read_Limit, errno = errno}
	}
	lowered := linux.RLimit{cur = soft, max = saved.max}
	if errno := linux.setrlimit(kind, &lowered); errno != .NONE {
		return Limit_Not_Lowered{kind = kind, step = .Lower_Limit, errno = errno}
	}
	return Limit_Lowered{kind = kind, saved = saved}
}

restore_limit :: proc(lowered: Limit_Lowered) {
	saved := lowered.saved
	linux.setrlimit(lowered.kind, &saved)
}
