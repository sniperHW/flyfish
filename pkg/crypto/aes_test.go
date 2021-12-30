package crypto

//go test -covermode=count -v -coverprofile=coverage.out -run=.

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestAES(t *testing.T) {
	key := []byte("example key 1234")
	plaintext := []byte("hello world")

	ciphertext, _ := AESCBCEncrypt(key, plaintext)

	plaintext, _ = AESCBCDecrypter(key, ciphertext)

	assert.Equal(t, "hello world", string(plaintext))

	key = fixKey([]byte(strings.Repeat("a", 33)))

	assert.Equal(t, 32, len(key))

	key = fixKey([]byte(strings.Repeat("a", 8)))

	assert.Equal(t, 16, len(key))

	key = fixKey([]byte(strings.Repeat("a", 17)))

	assert.Equal(t, 24, len(key))

	key = fixKey([]byte(strings.Repeat("a", 31)))

	assert.Equal(t, 32, len(key))

}
