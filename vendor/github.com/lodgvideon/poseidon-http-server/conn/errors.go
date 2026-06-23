package conn

import (
	"fmt"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// connError is a connection-fatal error returned by frame handler methods.
// When returned from a Handler callback the connection reader loop will
// send GOAWAY with the embedded error code and tear down the connection.
type connError struct {
	code frame.ErrCode
	msg  string
}

func (e connError) Error() string {
	return fmt.Sprintf("conn: connection error code=%v: %s", e.code, e.msg)
}
