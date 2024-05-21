package encryption

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
)

func GenerateRSAPrivateKey() (*rsa.PrivateKey, error) {
	// Generate RSA private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)

	if err != nil {
		panic(err)
	}

	// Save private key to a file
	privateKeyFile, err := os.Create("private_key.pem")

	if err != nil {
		panic(err)
	}

	defer privateKeyFile.Close()

	privateKeyPEM := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}

	if err := pem.Encode(privateKeyFile, privateKeyPEM); err != nil {
		panic(err)
	}

	return privateKey, err
}

func ReadPrivateKey() *rsa.PrivateKey {
	// Read private key from file
	privateKeyBytes, err := os.ReadFile("private_key.pem")

	if err != nil {

		privateKey, _ := GenerateRSAPrivateKey()

		return privateKey
	}

	// Decode PEM block containing private key
	block, _ := pem.Decode(privateKeyBytes)

	if block == nil {
		panic("failed to decode PEM block containing private key")
	}

	// Parse the private key
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)

	if err != nil {
		panic(err)
	}

	return privateKey
}
