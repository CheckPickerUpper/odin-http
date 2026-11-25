#+build linux
package server_tests

import "core:sync"

// A test in this package reads a descriptor number's state before and after serve, so each test
// that starts a server holds this lock while it runs.
process_resources_lock: sync.Mutex
