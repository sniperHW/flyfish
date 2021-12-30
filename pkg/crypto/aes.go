package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
)

func padding(ciphertext []byte, blockSize int) []byte {
	paddingSize := blockSize - (len(ciphertext)+4)%blockSize
	ret := make([]byte, 4, len(ciphertext)+4+paddingSize)
	binary.BigEndian.PutUint32(ret, uint32(len(ciphertext)))
	ret = append(ret, ciphertext...)
	padding := blockSize - len(ret)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ret, padtext...)
}

/*
   AES  CBC 加密
   key:加密key
   plaintext：加密明文
   ciphertext:解密返回字节字符串[ 整型以十六进制方式显示]
*/

func AESCBCEncrypt(keybyte, plainbyte []byte) (cipherbyte []byte, err error) {

	plainbyte = padding(plainbyte, aes.BlockSize)

	block, err := aes.NewCipher(keybyte)
	if err != nil {
		return
	}

	cipherbyte = make([]byte, aes.BlockSize+len(plainbyte))
	iv := cipherbyte[:aes.BlockSize]
	if _, err = io.ReadFull(rand.Reader, iv); err != nil {
		return
	}

	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(cipherbyte[aes.BlockSize:], plainbyte)
	return
}

/*
   AES  CBC 解码
   key:解密key
   ciphertext:加密返回的串
   plaintext：解密后的字符串
*/
func AESCBCDecrypter(keybyte, cipherbyte []byte) (plainbyte []byte, err error) {
	block, err := aes.NewCipher(keybyte)
	if err != nil {
		return
	}
	if len(cipherbyte) < aes.BlockSize {
		err = errors.New("ciphertext too short")
		return
	}

	iv := cipherbyte[:aes.BlockSize]
	cipherbyte = cipherbyte[aes.BlockSize:]
	if len(cipherbyte)%aes.BlockSize != 0 {
		err = errors.New("ciphertext is not a multiple of the block size")
		return
	}

	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(cipherbyte, cipherbyte)

	size := int(binary.BigEndian.Uint32(cipherbyte[:4]))

	plainbyte = cipherbyte[4 : 4+size]

	return
}
