package document

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"updater/internal/entity/document"
)

type Repo struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Repo {
	return &Repo{db: db}
}

func (r *Repo) UpsertDoc(ctx context.Context, doc *document.Model) error {

	return nil
}

func (r *Repo) GetByURL(ctx context.Context, url string) (*document.Model, error) {

	return nil, nil
}

func (r *Repo) Tx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	txDB, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	defer func() {
		txDB.Rollback(ctx)
	}()

	if err != nil {
		return err
	}

	err = fn(ctx, txDB)
	if err != nil {
		return err
	}

	return txDB.Commit(ctx)
}
