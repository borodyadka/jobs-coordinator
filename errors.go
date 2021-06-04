package coordinator

import (
	"errors"
)

var (
	ErrJobAlreadyExists = errors.New("job already exists")
	ErrJobNotExists     = errors.New("job not exists")
	ErrJobReleased      = errors.New("job already released")
	ErrJobTaken         = errors.New("job taken")
)
