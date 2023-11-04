package api

import (
	chi "github.com/go-chi/chi/v5"
	"github.com/sadath-12/keywave/api/handler"
	"github.com/sadath-12/keywave/membership"
)

func CreateRouter(cluster membership.Cluster) *chi.Mux {
	r := chi.NewRouter()
	handler.NewKeyValueHandler().Register(r)
	handler.NewNodesHandler(cluster).Register(r)

	return r
}
