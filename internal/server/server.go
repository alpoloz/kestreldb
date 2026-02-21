package server

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"kestreldb/internal/engine"
	"kestreldb/internal/server/routes"
)

type Server struct {
	addr string
	db   *engine.DB
}

func New(addr string, db *engine.DB) *Server {
	return &Server{addr: addr, db: db}
}

func (s *Server) ListenAndServe() error {
	r := chi.NewRouter()
	api := routes.New(s.db)

	r.Get("/v1/ping", ping)
	api.RegisterHash(r)
	api.RegisterZSet(r)

	httpServer := &http.Server{
		Addr:    s.addr,
		Handler: r,
	}

	if err := httpServer.ListenAndServe(); err != nil {
		return fmt.Errorf("listen and serve: %w", err)
	}
	return nil
}

type pingResponse struct {
	Message string `json:"message"`
}

func ping(w http.ResponseWriter, r *http.Request) {
	render.JSON(w, r, pingResponse{Message: "PONG"})
}
