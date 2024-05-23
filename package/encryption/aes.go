package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
)

var (
	ErrInvalidEncryptionKey = errors.New("invalid encryption key")
	ErrCipherTextTooShort   = errors.New("ciphertext too short")
	ErrInvalidCipherText    = errors.New("invalid ciphertext")
)

// Encryptor handles message encryption and decryption
type Encryptor interface {
	Encrypt(text string) (string, error)
	Decrypt(text string) (string, error)
}

// AESEncryptor provides AES encryption
type AESEncryptor struct {
	key []byte
}

// NewAESEncryptor creates a new AESEncryptor
func NewAESEncryptor(key string) (*AESEncryptor, error) {
	if len(key) != 32 {
		return nil, ErrInvalidEncryptionKey
	}
	return &AESEncryptor{key: []byte(key)}, nil
}

// Encrypt encrypts the given text
func (e *AESEncryptor) Encrypt(text string) (string, error) {
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return "", err
	}

	ciphertext := make([]byte, aes.BlockSize+len(text))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
	}

	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], []byte(text))

	return base64.URLEncoding.EncodeToString(append(iv, ciphertext...)), nil
}

// Decrypt decrypts the given text
func (e *AESEncryptor) Decrypt(text string) (string, error) {
	ciphertext, err := base64.URLEncoding.DecodeString(text)
	if err != nil {
		return "", ErrInvalidCipherText
	}

	block, err := aes.NewCipher(e.key)
	if err != nil {
		return "", err
	}

	if len(ciphertext) < aes.BlockSize {
		return "", ErrCipherTextTooShort
	}

	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(ciphertext, ciphertext)

	return string(ciphertext), nil
}
