package client

import (
	"errors"
	"fmt"
)

func newError(message string, argumrnts ...any) error {
	if len(argumrnts) == 0 {
		return errors.New(message)
	}
	return fmt.Errorf(message, argumrnts...)
}

func newErrWrongNumberOfArguments(commandName string) error {
	return newError("ERR wrong number of arguments for '%s' command", commandName)
}
