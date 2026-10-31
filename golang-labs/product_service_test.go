package main

import (
	"context"
	"errors"
	"testing"
)

type fakeProductRepository struct {
	product *Product
	err     error
	ctx     context.Context
	id      int64
	calls   int
}

var _ ProductFinder = (*fakeProductRepository)(nil)

func (r *fakeProductRepository) FindByID(ctx context.Context, id int64) (*Product, error) {
	r.ctx, r.id = ctx, id
	r.calls++
	return r.product, r.err
}

func TestProductServiceGetProduct(t *testing.T) {
	wantProduct := &Product{ID: 42, Name: "Keyboard", Price: 100}
	wantErr := errors.New("repository unavailable")
	for _, tt := range []struct {
		name    string
		product *Product
		err     error
	}{
		{name: "successful lookup", product: wantProduct},
		{name: "repository error", err: wantErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			repo := &fakeProductRepository{product: tt.product, err: tt.err}
			service := NewProductService(repo)

			got, err := service.GetProduct(ctx, 42)

			if got != tt.product {
				t.Errorf("GetProduct() product = %v; want %v", got, tt.product)
			}
			if !errors.Is(err, tt.err) {
				t.Errorf("GetProduct() error = %v; want %v", err, tt.err)
			}
			if repo.calls != 1 || repo.ctx != ctx || repo.id != 42 {
				t.Errorf("repository received calls=%d, context=%v, ID=%d; want one call with original context and ID 42", repo.calls, repo.ctx, repo.id)
			}
		})
	}
}
