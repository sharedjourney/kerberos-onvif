package stream

import "fmt"

// Op identifies which Stream operation failed. Used by ErrPullFailed,
// ErrRenewFailed and ErrRecreateFailed so consumers can branch with
// errors.As without parsing the wrapped message.
type Op string

const (
	OpPull     Op = "pull"
	OpRenew    Op = "renew"
	OpRecreate Op = "recreate"
)

// ErrPullFailed wraps a transient PullMessages failure. The pull loop
// surfaces it on the Errors channel and continues. Consumers can match
// with errors.As(err, &stream.ErrPullFailed{}) — see
// TestErrors_TypedAssertion.
type ErrPullFailed struct{ Err error }

func (e ErrPullFailed) Error() string { return fmt.Sprintf("pull messages: %v", e.Err) }
func (e ErrPullFailed) Unwrap() error { return e.Err }
func (ErrPullFailed) Op() Op          { return OpPull }

// ErrRenewFailed wraps a Renew SOAP failure. Renew errors are usually
// recovered implicitly: the subscription dies, pull starts failing, and
// the reconnect logic recreates it.
type ErrRenewFailed struct{ Err error }

func (e ErrRenewFailed) Error() string { return fmt.Sprintf("renew pull point: %v", e.Err) }
func (e ErrRenewFailed) Unwrap() error { return e.Err }
func (ErrRenewFailed) Op() Op          { return OpRenew }

// ErrRecreateFailed wraps a failed CreatePullPointSubscription during
// the reconnect path. The loop continues with exponential backoff;
// consumers seeing this repeatedly should consider the camera offline.
type ErrRecreateFailed struct{ Err error }

func (e ErrRecreateFailed) Error() string { return fmt.Sprintf("recreate pull point: %v", e.Err) }
func (e ErrRecreateFailed) Unwrap() error { return e.Err }
func (ErrRecreateFailed) Op() Op          { return OpRecreate }
