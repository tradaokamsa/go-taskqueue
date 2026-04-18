package domain

import "errors"

var (
	ErrNotFound          = errors.New("not found")
	ErrAlreadyClaimed    = errors.New("job already claimed")
	ErrInvalidTransition = errors.New("invalid state transition")
)
