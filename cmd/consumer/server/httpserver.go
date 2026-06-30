package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/cauan745/trabalho_kafka/internal/app/auth"
	appdatabase "github.com/cauan745/trabalho_kafka/internal/app/database"
	"github.com/cauan745/trabalho_kafka/internal/kafka/producer"
)

type HttpServer struct {
	db       appdatabase.Database
	jwtMaker auth.JWTMaker
	producer *producer.KafkaProducer
}

func StartHttpServer(db *appdatabase.Database, prod *producer.KafkaProducer) {
	port := ":8080"

	jwtMaker := auth.NewJWTMaker("teste")

	s := HttpServer{*db, *jwtMaker, prod}

	mux := http.NewServeMux()

	server := http.Server{
		Addr:    port,
		Handler: mux,
	}

	fs := http.FileServer(http.Dir("./static"))

	mux.HandleFunc("POST /api/register", s.userRegister)
	mux.HandleFunc("POST /api/login", s.userLogin)
	mux.Handle("POST /api/ride/start", s.jwtMiddleware(http.HandlerFunc(s.startRide)))
	mux.Handle("GET /api/rides", s.jwtMiddleware(http.HandlerFunc(s.getRides)))
	mux.Handle("DELETE /api/rides/{id}", s.jwtMiddleware(http.HandlerFunc(s.deleteRide)))
	mux.Handle("GET /app/", s.jwtMiddleware(fs))

	mux.Handle("GET /", fs)

	fmt.Println("HttpServer running on port", port)

	err := server.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}

func sendToken(w http.ResponseWriter, token string, duration time.Duration) {
	cookie := &http.Cookie{
		Name:     "session_token",
		Value:    token,
		Path:     "/",
		Expires:  time.Now().Add(duration),
		HttpOnly: true,
		Secure:   false, // false to allow non-https connections
		SameSite: http.SameSiteLaxMode,
	}

	http.SetCookie(w, cookie)
	w.Write([]byte("Cookie has been set"))
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

	duration := 60 * time.Minute

	token, err := s.jwtMaker.CreateToken(id, user.Name, false, duration)
	if err != nil {
		http.Error(w, "", 500)
		return
	}

	sendToken(w, token, duration)
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
		http.Error(w, "invalid name or password", 500)
		return
	}

	duration := 60 * time.Minute

	token, err := s.jwtMaker.CreateToken(id, user.Name, false, duration)
	if err != nil {
		http.Error(w, "", 500)
		return
	}

	sendToken(w, token, duration)
}

func (s *HttpServer) startRide(w http.ResponseWriter, r *http.Request) {
	type RideRequest struct {
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
	}

	var req RideRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid json", 400)
		return
	}

	userId, ok := r.Context().Value("user_id").(int)
	if !ok {
		http.Error(w, "Unauthorized", 401)
		return
	}

	rideId, err := s.db.NewRide(fmt.Sprint(userId), "")
	if err != nil {
		http.Error(w, "Failed to create ride", 500)
		return
	}

	type Passenger struct {
		PassengerId float64 `json:"passengerId"`
		RideId      int     `json:"rideId"`
		Latitude    float64 `json:"latitude"`
		Longitude   float64 `json:"longitude"`
	}

	pas := Passenger{
		PassengerId: float64(userId),
		RideId:      rideId,
		Latitude:    req.Latitude,
		Longitude:   req.Longitude,
	}

	js, err := json.Marshal(pas)
	if err != nil {
		http.Error(w, "Failed to marshal ride request", 500)
		return
	}

	s.producer.Produce(string(js))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"driver requested"}`))
}

func (s *HttpServer) getRides(w http.ResponseWriter, r *http.Request) {
	userId, ok := r.Context().Value("user_id").(int)
	if !ok {
		http.Error(w, "Unauthorized", 401)
		return
	}

	rides, err := s.db.GetRidesByPassengerId(fmt.Sprint(userId))
	if err != nil {
		http.Error(w, "Failed to get rides", 500)
		return
	}

	if rides == nil {
		rides = []appdatabase.Ride{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rides)
}

func (s *HttpServer) deleteRide(w http.ResponseWriter, r *http.Request) {
	userId, ok := r.Context().Value("user_id").(int)
	if !ok {
		http.Error(w, "Unauthorized", 401)
		return
	}

	idStr := r.PathValue("id")
	var id int
	_, err := fmt.Sscanf(idStr, "%d", &id)
	if err != nil {
		http.Error(w, "Invalid ride ID", 400)
		return
	}

	err = s.db.SoftDeleteRide(id, fmt.Sprint(userId))
	if err != nil {
		http.Error(w, "Failed to delete ride", 500)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success"}`))
}

