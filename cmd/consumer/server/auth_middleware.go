package main

import (
	"net/http"
)

func (s HttpServer) jwtMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")

		cookie, err := r.Cookie("session_token")
		if err != nil {
			//http.Error(w, "token not found", 400)
			http.Redirect(w, r, "/login.html", http.StatusSeeOther)
			return
		}

		if cookie.Value == "" {
			http.Redirect(w, r, "/login.html", http.StatusSeeOther)
			return
		}

		err = s.jwtMaker.VerifyToken(cookie.Value)
		if err != nil {
			//http.Error(w, "invalid token", 400)
			http.Redirect(w, r, "/login.html", http.StatusSeeOther)
			return
		}

		next.ServeHTTP(w, r)
	})
}
