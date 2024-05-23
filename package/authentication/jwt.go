package authentication

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"go_core_api/package/encryption"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type JWT struct {
	Expire        int64 // Define expiration time for access token
	RefreshExpire int64 // Define expiration time for refresh token
	privateKey    *rsa.PrivateKey
}

func (j *JWT) InitJWT() {
	privateKey := encryption.ReadPrivateKey()

	j.privateKey = privateKey
}

func (j *JWT) GenerateToken(address string) (accessToken, refreshToken string, err error) {
	// Access Token Claims
	accessClaims := jwt.MapClaims{
		"exp": time.Now().Add(time.Minute * time.Duration(j.Expire)).Unix(),
	}

	accessToken, err = j.generateTokenWithClaims(accessClaims)

	if err != nil {
		return "", "", errors.New("failed to create access token")
	}

	// Refresh Token Claims
	refreshClaims := jwt.MapClaims{
		"address": address,
		"exp":     time.Now().Add(time.Hour * time.Duration(j.RefreshExpire)).Unix(),
	}

	refreshToken, err = j.generateTokenWithClaims(refreshClaims)

	if err != nil {
		return "", "", errors.New("failed to create refresh token")
	}

	return accessToken, refreshToken, nil
}

func (j *JWT) generateTokenWithClaims(claims jwt.Claims) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)

	return token.SignedString(j.privateKey)
}

func (j *JWT) VerifyRefreshToken(tokenString string) (string, error) {
	tokenString = removeBearerPrefix(tokenString)

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return &j.privateKey.PublicKey, nil
	})

	if err != nil {
		return "", fmt.Errorf("failed to parse token: %v", err)
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		address := claims["address"].(string)
		return address, nil
	}

	return "", errors.New("refresh token invalid")
}

// removeBearerPrefix removes "Bearer " prefix from token.
func removeBearerPrefix(token string) string {
	return strings.TrimPrefix(token, "Bearer ")
}

func (j *JWT) RefreshAccessToken(refreshToken string) (string, error) {
	address, err := j.VerifyRefreshToken(refreshToken)

	if err != nil {
		return "", err
	}

	accessToken, _, err := j.GenerateToken(address)

	if err != nil {
		return "", err
	}

	return accessToken, nil
}
