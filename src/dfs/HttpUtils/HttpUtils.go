package HttpUtils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Handler = func(map[string]any) (int, map[string]any)
type HttpAPIBuilder struct {
	paths map[string]map[string]Handler
}

func NewHttpAPIBuilder() *HttpAPIBuilder {
	return &HttpAPIBuilder{paths: make(map[string]map[string]Handler)}
}

func (b *HttpAPIBuilder) RegisterHandler(path string, method string, handler Handler) {
	handlers, ok := b.paths[path]
	if !ok {
		handlers = make(map[string]Handler)
		b.paths[path] = handlers
	}
	handlers[method] = handler
}

func (b *HttpAPIBuilder) Build() {
	for path, handlers := range b.paths {
		http.HandleFunc(path, func(writer http.ResponseWriter, request *http.Request) {
			handler, ok := handlers[request.Method]
			if !ok {
				writer.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			if request.Body == nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			data, err := io.ReadAll(request.Body)
			if err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			var reqBody map[string]any
			err = json.Unmarshal(data, &reqBody)
			if err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			// call the actual handler
			statusCode, result := handler(reqBody)
			writer.WriteHeader(statusCode)
			resultBytes, err := json.Marshal(result)
			if err != nil {
				fmt.Println(err)
			}
			_, err = writer.Write(resultBytes)
			if err != nil {
				fmt.Println(err)
			}
		})
	}
}
