package api

import (
	chi "github.com/go-chi/chi/v5"
	"github.com/sadath-12/keywave/api/handler"
)

func CreateRouter() *chi.Mux {
	r := chi.NewRouter()
	handler.NewKeyValueHandler().Register(r)

	return r
}