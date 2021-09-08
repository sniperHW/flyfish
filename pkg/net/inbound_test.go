package net

//go test -tags=aio -covermode=count -v -coverprofile=coverage.out -run=.
//go test -tags=bio -covermode=count -v -coverprofile=coverage.out -run=.

/*import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/net/pb"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
)

func test_unpack(_ *pb.Namespace, buff []byte, r int, w int) (interface{}, int, error) {

	unpackSize := w - r
	if unpackSize > 4 {
		reader := buffer.NewReader(b[r : r+unpackSize])
		payload := int(reader.GetUint32())

		if payload == 0 {
			return nil, 0, fmt.Errorf("zero payload")
		}

		totalSize := payload + 4

		packetSize = totalSize

		if totalSize <= unpackSize {
			msg, err := reader.CopyBytes(payload)
		}
	}
	return

}

func Test(t *testing.T) {

}*/
