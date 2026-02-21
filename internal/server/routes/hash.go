package routes

import (
	"net/http"
	"sort"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

func (a *API) RegisterHash(r chi.Router) {
	r.Put("/v1/hashes/{key}/fields/{field}", a.hashSet)
	r.Get("/v1/hashes/{key}/fields/{field}", a.hashGet)
	r.Delete("/v1/hashes/{key}/fields/{field}", a.hashDelete)
	r.Get("/v1/hashes/{key}/length", a.hashLen)
	r.Get("/v1/hashes/{key}", a.hashGetAll)
}

type hashSetRequest struct {
	Value string `json:"value"`
}

type hashSetResponse struct {
	Created bool `json:"created"`
}

type hashGetResponse struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
}

type hashDeleteResponse struct {
	Deleted bool `json:"deleted"`
}

type hashLenResponse struct {
	Length uint64 `json:"length"`
}

type hashEntry struct {
	Field string `json:"field"`
	Value string `json:"value"`
}

type hashGetAllResponse struct {
	Entries []hashEntry `json:"entries"`
}

func (a *API) hashSet(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	field := chi.URLParam(r, "field")

	var req hashSetRequest
	if err := decodeJSON(r, &req); err != nil {
		renderError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	created := a.db.HSet(key, field, req.Value) == 1
	render.JSON(w, r, hashSetResponse{Created: created})
}

func (a *API) hashGet(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	field := chi.URLParam(r, "field")

	value, found := a.db.HGet(key, field)
	render.JSON(w, r, hashGetResponse{
		Value: value,
		Found: found,
	})
}

func (a *API) hashDelete(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	field := chi.URLParam(r, "field")

	deleted := a.db.HDel(key, field) == 1
	render.JSON(w, r, hashDeleteResponse{Deleted: deleted})
}

func (a *API) hashLen(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	render.JSON(w, r, hashLenResponse{Length: uint64(a.db.HLen(key))})
}

func (a *API) hashGetAll(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	pairs := a.db.HGetAll(key)
	fields := make([]string, 0, len(pairs))
	for field := range pairs {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	entries := make([]hashEntry, 0, len(fields))
	for _, field := range fields {
		entries = append(entries, hashEntry{
			Field: field,
			Value: pairs[field],
		})
	}
	render.JSON(w, r, hashGetAllResponse{Entries: entries})
}
