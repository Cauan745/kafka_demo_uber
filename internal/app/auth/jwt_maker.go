package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

type JWTMaker struct {
	secretKey string
}

type Claims struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
	jwt.RegisteredClaims
}

func NewJWTMaker(secretKey string) *JWTMaker {
	return &JWTMaker{secretKey}
}

func CreateClaim(id int, name string, duration time.Duration) Claims {
	tokenId, _ := uuid.NewRandom()
	return Claims{
		Id:   id,
		Name: name,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        tokenId.String(),
			Subject:   name,
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(duration)),
		},
	}
}

func (maker *JWTMaker) CreateToken(id int, name string, isAdmin bool, duration time.Duration) (string, error) {
	claims := CreateClaim(id, name, duration)

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, err := token.SignedString([]byte(maker.secretKey))
	if err != nil {
		return "", fmt.Errorf("error signing token: %w", err)
	}

	return tokenStr, nil
}

func (maker *JWTMaker) VerifyToken(tokenStr string) error {
	_, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
		// verify the signing method
		_, ok := token.Method.(*jwt.SigningMethodHMAC)
		if !ok {
			return nil, fmt.Errorf("invalid token signing method")
		}

		return []byte(maker.secretKey), nil
	})

	return err
}

func (maker *JWTMaker) GetTokenClaims(tokenStr string) (*Claims, error) {
	c := &Claims{}
	_, err := jwt.ParseWithClaims(tokenStr, c, func(token *jwt.Token) (interface{}, error) {
		_, ok := token.Method.(*jwt.SigningMethodHMAC)
		if !ok {
			return nil, fmt.Errorf("invalid token signing method")
		}
		return []byte(maker.secretKey), nil
	})

	return c, err
}
