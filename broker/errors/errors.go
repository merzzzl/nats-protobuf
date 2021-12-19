package errors

import "fmt"

type brokerErrorType int32

const (
	ErrACK brokerErrorType = 0
	ErrNAK brokerErrorType = 1
)

var DefaultErr = ErrACK

type brokerError struct {
	errorType brokerErrorType
	info      string
}

func NewError(errType brokerErrorType, s string) error {
	return &brokerError{
		errorType: errType,
		info:      s,
	}
}

func NewErrorf(errType brokerErrorType, format string, a ...interface{}) error {
	return &brokerError{
		errorType: errType,
		info:      fmt.Sprintf(format, a...),
	}
}

func (e *brokerError) Error() string {
	return e.info
}

func ErrorType(err error) brokerErrorType {
	if bErr, ok := err.(*brokerError); ok {
		return bErr.errorType
	}
	return DefaultErr
}
