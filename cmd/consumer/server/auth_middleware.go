package main

import (
	"net/http"
)

func (s HttpServer) jwtMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("session_token")
		if err != nil {
			http.Error(w, "token not found", 400)
			return
		}

		err = s.jwtMaker.VerifyToken(cookie.Value)
		if err != nil {
			http.Error(w, "invalid token", 400)
			return
		}

		next.ServeHTTP(w, r)
	})
}
