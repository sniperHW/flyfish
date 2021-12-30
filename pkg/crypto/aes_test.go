package crypto

//go test -covermode=count -v -coverprofile=coverage.out -run=.

import (
	//"encoding/binary"
	//"fmt"
	//"github.com/sniperHW/flyfish/pkg/buffer"
	//"github.com/sniperHW/flyfish/pkg/net/pb"
	"github.com/stretchr/testify/assert"
	//"strings"
	"testing"
)

func TestAES(t *testing.T) {
	key := []byte("example key 1234")
	plaintext := []byte("hello world")

	ciphertext := AESCBCEncrypt(key, plaintext)

	//fmt.Println(ciphertext)

	plaintext = AESCBCDecrypter(key, ciphertext)

	assert.Equal(t, "hello world", string(plaintext))

}
