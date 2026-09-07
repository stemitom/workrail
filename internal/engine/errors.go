package engine

import "errors"

// ErrPermanent marks a failure that retrying cannot fix — a rejected account,
// a malformed request, a business rule that will refuse the same way every
// time. Jobs failing with a permanent error go straight to dead_letter with
// their remaining attempts unspent, so an operator sees them immediately
// instead of after several pointless retries against a live provider.
var ErrPermanent = errors.New("permanent failure")

// Permanent wraps err so the worker dead-letters the job instead of retrying
// it. The returned error keeps err's message and unwraps to it, so callers can
// still match the underlying cause.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent reports whether err (or anything it wraps) was marked permanent.
func IsPermanent(err error) bool {
	return errors.Is(err, ErrPermanent)
}

type permanentError struct {
	err error
}

func (e permanentError) Error() string { return e.err.Error() }

func (e permanentError) Unwrap() error { return e.err }

func (e permanentError) Is(target error) bool { return target == ErrPermanent }
