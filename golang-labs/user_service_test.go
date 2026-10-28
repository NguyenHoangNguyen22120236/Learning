package main

import (
	"context"
	"errors"
	"testing"
)

type userRepositoryFunc func(context.Context, int64) (*User, error)

func (f userRepositoryFunc) FindByID(ctx context.Context, id int64) (*User, error) {
	return f(ctx, id)
}

func TestGetUser(t *testing.T) {
	wantUser := &User{ID: 42, Name: "Alice"}
	wantErr := errors.New("repository unavailable")
	for _, tt := range []struct {
		name string
		user *User
		err  error
	}{
		{name: "found", user: wantUser},
		{name: "repository error", err: wantErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			called := false
			repo := userRepositoryFunc(func(gotCtx context.Context, id int64) (*User, error) {
				called = true
				if gotCtx != ctx || id != 42 {
					t.Fatalf("repository received context %v and ID %d; want original context and ID 42", gotCtx, id)
				}
				return tt.user, tt.err
			})
			got, err := NewUserService(repo).GetUser(ctx, 42)
			if !called || got != tt.user || err != tt.err {
				t.Fatalf("GetUser() = (%v, %v), repository called = %v; want (%v, %v), called = true", got, err, called, tt.user, tt.err)
			}
		})
	}
}
