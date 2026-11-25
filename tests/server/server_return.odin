package server_tests

import "core:net"

Listen_Failed :: struct #all_or_none {
	error: net.Network_Error,
}

Serve_Returned :: struct #all_or_none {
	error: net.Network_Error,
}

Server_Return :: union #no_nil {
	Listen_Failed,
	Serve_Returned,
}
