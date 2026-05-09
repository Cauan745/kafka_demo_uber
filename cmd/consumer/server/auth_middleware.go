package main

import (
	"net/http"
	"strings"
)

func (s HttpServer) jwtMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")

		token := strings.Replace(header, "Bearer ", "", 1)
		err := s.jwtMaker.VerifyToken(token)
		if err != nil {
			http.Error(w, "invalid token", 400)
			return
		}

		next.ServeHTTP(w, r)
	})
}
