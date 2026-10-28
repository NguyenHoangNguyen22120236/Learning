package main

import "context"

type User struct {
	ID   int64
	Name string
}

type UserRepository interface {
	FindByID(ctx context.Context, id int64) (*User, error)
}

type UserGetter interface {
	GetUser(ctx context.Context, id int64) (*User, error)
}

type UserService struct {
	repo UserRepository
}

var _ UserGetter = (*UserService)(nil)

func NewUserService(repo UserRepository) *UserService {
	return &UserService{repo: repo}
}

func (s *UserService) GetUser(ctx context.Context, id int64) (*User, error) {
	return s.repo.FindByID(ctx, id)
}
