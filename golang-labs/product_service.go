package main

import "context"

type Product struct {
	ID    int64
	Name  string
	Price int64
}

// ProductFinder describes the repository behavior ProductService consumes.
type ProductFinder interface {
	FindByID(ctx context.Context, id int64) (*Product, error)
}

type ProductService struct {
	finder ProductFinder
}

func NewProductService(finder ProductFinder) *ProductService {
	return &ProductService{finder: finder}
}

func (s *ProductService) GetProduct(ctx context.Context, id int64) (*Product, error) {
	return s.finder.FindByID(ctx, id)
}
