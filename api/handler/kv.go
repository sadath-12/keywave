package handler

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"

	"github.com/sadath-12/keywave/api/model"
	"github.com/sadath-12/keywave/nodeapi/grpc"
	storagepb "github.com/sadath-12/keywave/storage/proto"
)

type KeyValueHandler struct {
}

func NewKeyValueHandler() *KeyValueHandler {
	return &KeyValueHandler{}
}

func (api *KeyValueHandler) Register(r chi.Router) {
	r.Get("/kv/{key}", api.getKey)
	r.Put("/kv/{key}", api.putKey)
}

func (api *KeyValueHandler) putKey(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	ctx := context.Background()
	conn, err := grpc.Dial(ctx, "127.0.0.1:3000")

	if err != nil {
		fmt.Println("err: ",err)
	}

	var params model.PutKeyParams
	if err := render.DecodeJSON(r.Body, &params); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Println("connection putting ----------------")
	res, err := conn.Put(ctx, &storagepb.PutRequest{
		Key: key,
		Value: &storagepb.VersionedValue{
			Data:    []byte(params.Value),
			Version: params.Version,
		},
	})

	if err != nil {
		fmt.Println("err", err)
	}
	fmt.Println("connection putted ----------------")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	render.JSON(w, r, &model.PutKeyResponse{
		Acknowledged: 1,
		Version:      res.Version,
	})
}

func (api *KeyValueHandler) getKey(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	ctx := context.Background()
	conn, err := grpc.Dial(ctx, "127.0.0.1:3000")

	if err != nil {
		fmt.Println("cant dial ")
	}

	res, err := conn.Get(ctx, &storagepb.GetRequest{
		Key: key,
	})

	response := model.GetKeyResponse{
		Version: "1",
		Values:  []string{string(res.Value[0].Data)},
		Exists:  true,
		Value:   string(string(res.Value[0].Data)),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	render.JSON(w, r, response)
}
