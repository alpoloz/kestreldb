package routes

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

func (a *API) RegisterZSet(r chi.Router) {
	r.Put("/v1/sorted-sets/{key}/members/{member}", a.sortedSetAdd)
	r.Delete("/v1/sorted-sets/{key}/members/{member}", a.sortedSetRemove)
	r.Get("/v1/sorted-sets/{key}/members/{member}", a.sortedSetScore)
	r.Get("/v1/sorted-sets/{key}/cardinality", a.sortedSetCardinality)
	r.Get("/v1/sorted-sets/{key}/range", a.sortedSetRange)
}

type sortedSetAddRequest struct {
	Score float64 `json:"score"`
}

type sortedSetAddResponse struct {
	Added bool `json:"added"`
}

type sortedSetRemoveResponse struct {
	Removed bool `json:"removed"`
}

type sortedSetScoreResponse struct {
	Score float64 `json:"score"`
	Found bool    `json:"found"`
}

type sortedSetCardinalityResponse struct {
	Count uint64 `json:"count"`
}

type sortedSetMember struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

type sortedSetRangeResponse struct {
	Items []sortedSetMember `json:"items"`
}

func (a *API) sortedSetAdd(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	member := chi.URLParam(r, "member")

	var req sortedSetAddRequest
	if err := decodeJSON(r, &req); err != nil {
		renderError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	added := a.db.ZAdd(key, req.Score, member) == 1
	render.JSON(w, r, sortedSetAddResponse{Added: added})
}

func (a *API) sortedSetRemove(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	member := chi.URLParam(r, "member")

	removed := a.db.ZRem(key, member) == 1
	render.JSON(w, r, sortedSetRemoveResponse{Removed: removed})
}

func (a *API) sortedSetScore(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	member := chi.URLParam(r, "member")

	score, found := a.db.ZScore(key, member)
	render.JSON(w, r, sortedSetScoreResponse{
		Score: score,
		Found: found,
	})
}

func (a *API) sortedSetCardinality(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	render.JSON(w, r, sortedSetCardinalityResponse{Count: uint64(a.db.ZCard(key))})
}

func (a *API) sortedSetRange(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")

	startQuery := r.URL.Query().Get("start")
	stopQuery := r.URL.Query().Get("stop")

	start64, err := strconv.ParseInt(startQuery, 10, 64)
	if err != nil {
		renderError(w, r, http.StatusBadRequest, "invalid start")
		return
	}

	stop64, err := strconv.ParseInt(stopQuery, 10, 64)
	if err != nil {
		renderError(w, r, http.StatusBadRequest, "invalid stop")
		return
	}

	start, err := int64ToInt(start64)
	if err != nil {
		renderError(w, r, http.StatusBadRequest, fmt.Sprintf("start: %s", err))
		return
	}

	stop, err := int64ToInt(stop64)
	if err != nil {
		renderError(w, r, http.StatusBadRequest, fmt.Sprintf("stop: %s", err))
		return
	}

	items := a.db.ZRange(key, start, stop)
	out := make([]sortedSetMember, 0, len(items))
	for _, item := range items {
		out = append(out, sortedSetMember{
			Member: item.Member,
			Score:  item.Score,
		})
	}
	render.JSON(w, r, sortedSetRangeResponse{Items: out})
}
