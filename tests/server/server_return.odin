#+build linux
package server_tests

import http "../.."

Listen_Failed :: struct #all_or_none {
	error: http.Server_Error,
}

Serve_Returned :: struct #all_or_none {
	error: http.Server_Error,
}

Server_Return :: union #no_nil {
	Listen_Failed,
	Serve_Returned,
}
