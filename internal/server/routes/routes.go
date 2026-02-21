package routes

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/render"
	"kestreldb/internal/engine"
)

type API struct {
	db *engine.DB
}

func New(db *engine.DB) *API {
	return &API{db: db}
}

type errorResponse struct {
	Error string `json:"error"`
}

func renderError(w http.ResponseWriter, r *http.Request, code int, message string) {
	render.Status(r, code)
	render.JSON(w, r, errorResponse{Error: message})
}

func decodeJSON(r *http.Request, dst any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	return nil
}

func int64ToInt(v int64) (int, error) {
	const (
		maxInt = int64(^uint(0) >> 1)
		minInt = -maxInt - 1
	)
	if v < minInt || v > maxInt {
		return 0, errors.New("value out of range for int")
	}
	return int(v), nil
}
