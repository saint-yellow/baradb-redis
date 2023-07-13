package ds

import "errors"

var (
	ErrWrongTypeOperation = errors.New(
		"wrong-type operation against a key holding the wrong kind of value",
	)
	ErrExpiredValue         = errors.New("the value is expired")
	ErrNilValue             = errors.New("the value is nill")
	ErrUnsupportedOperation = errors.New("unsupported operation")
	ErrInvalidInteger       = errors.New("value is not a valid integer or out of range")
	ErrInvalidFloat         = errors.New("value is not a valid float or out of range")
)
