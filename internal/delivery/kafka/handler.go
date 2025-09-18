package kafka

import (
	"context"
	"fmt"
	"updater/internal/entity/document"
)

// type Processor interface {
//   Process(doc *Model) (*Model, error)
// }

type Service interface {
	UpsertDoc(ctx context.Context, doc *document.Model) (*document.Model, error)
}

type Handler struct {
	service Service
}

func (h *Handler) Process(doc *document.Model) (*document.Model, error) {
	d, err := h.service.UpsertDoc(context.Background(), doc)
	if err != nil {
		return nil, fmt.Errorf("upsert doc: %w", err)
	}

	return d, nil
}
