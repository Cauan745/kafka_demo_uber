package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	appdatabase "github.com/cauan745/trabalho_kafka/internal/app/database"
)

type HttpServer struct {
	db appdatabase.Database
}

func StartHttpServer() {
	port := ":8080"

	db := appdatabase.New(5432, "kafka_uber", "localhost", "postgres", "password")
	db.CreateUserTable()

	s := HttpServer{*db}

	mux := http.NewServeMux()

	server := http.Server{
		Addr:    port,
		Handler: mux,
	}

	mux.HandleFunc("POST /api/register", s.userRegister)
	mux.HandleFunc("POST /api/login", s.userLogin)

	fmt.Println("HttpServer running on port", port)

	err := server.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}

func (s *HttpServer) userRegister(w http.ResponseWriter, r *http.Request) {
	user := appdatabase.User{}
	err := json.NewDecoder(r.Body).Decode(&user)
	if err != nil {
		http.Error(w, "Invalid json", 400)
		return
	}

	id, err := s.db.Register(user)
	if err != nil {
		http.Error(w, "", 500)
		return
	}

	w.WriteHeader(200)
	fmt.Fprintf(w, `{id:"%d"}`, id)
}

func (s *HttpServer) userLogin(w http.ResponseWriter, r *http.Request) {
	user := appdatabase.User{}
	err := json.NewDecoder(r.Body).Decode(&user)
	if err != nil {
		http.Error(w, "Invalid json", 400)
		return
	}

	id, err := s.db.Login(user)
	if err != nil {
		http.Error(w, "", 500)
		return
	}

	w.WriteHeader(200)
	fmt.Fprintf(w, `{id:"%d"}`, id)
}
