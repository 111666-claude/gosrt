package srt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrNoCandidates is returned by DialCandidates and
	// DialCandidatesWithContext when no candidate address has been given.
	ErrNoCandidates = errors.New("srt: no candidate addresses")

	// ErrInvalidCandidate is returned when one of the candidate addresses is
	// not a valid "host:port" address. The returned error identifies the
	// offending candidate by its position and value.
	ErrInvalidCandidate = errors.New("srt: invalid candidate address")

	// ErrAllCandidatesFailed is returned when every candidate address has been
	// attempted (or determined to be unusable) and none completed the SRT
	// handshake. The returned error is a *CandidatesError that names each
	// candidate and its failure.
	ErrAllCandidatesFailed = errors.New("srt: all candidate addresses failed")
)

// CandidateError reports why a single candidate address failed.
type CandidateError struct {
	// Address is the candidate address as it was given to DialCandidates.
	Address string

	// Err is the error that the attempt for this address returned.
	Err error
}

// Error implements the error interface.
func (e *CandidateError) Error() string {
	if e == nil {
		return "<nil>"
	}

	if e.Err == nil {
		return e.Address
	}

	return e.Address + ": " + e.Err.Error()
}

// Unwrap returns the underlying error of this candidate.
func (e *CandidateError) Unwrap() error {
	if e == nil {
		return nil
	}

	return e.Err
}

// CandidatesError reports the failures of all candidate addresses. It matches
// ErrAllCandidatesFailed with errors.Is, and the individual candidate errors
// can be inspected with errors.As.
type CandidatesError struct {
	// Attempts contains one entry for every candidate that was attempted, in
	// the order the candidates were given.
	Attempts []CandidateError
}

// Error implements the error interface.
func (e *CandidatesError) Error() string {
	if e == nil {
		return ErrAllCandidatesFailed.Error()
	}

	if len(e.Attempts) == 0 {
		return ErrAllCandidatesFailed.Error()
	}

	var b strings.Builder

	b.WriteString(ErrAllCandidatesFailed.Error())
	b.WriteString(":")

	for i := range e.Attempts {
		b.WriteString(" ")
		b.WriteString(e.Attempts[i].Error())
		b.WriteString(";")
	}

	return strings.TrimSuffix(b.String(), ";")
}

// Unwrap returns ErrAllCandidatesFailed and the individual candidate errors,
// so that errors.Is and errors.As can inspect them.
func (e *CandidatesError) Unwrap() []error {
	if e == nil {
		return nil
	}

	errs := make([]error, 0, len(e.Attempts)+1)
	errs = append(errs, ErrAllCandidatesFailed)

	for i := range e.Attempts {
		errs = append(errs, &e.Attempts[i])
	}

	return errs
}

// DialCandidates connects to the first candidate address that completes the
// SRT handshake and returns that connection. It is meant for setups with a
// primary and one or more backup SRT entry points: a primary that silently
// drops packets (a blackhole) therefore does not delay the startup for longer
// than interval.
//
// The addresses are of the form "host:port". Duplicate addresses are collapsed
// so that every unique address is attempted at most once. If an address is not
// a valid "host:port" address, no connection attempt is made at all and an
// error matching ErrInvalidCandidate is returned. If all candidates fail, the
// returned error matches ErrAllCandidatesFailed and its message names every
// candidate and why it failed. That is also true when only a single unique
// address is left after collapsing duplicates.
//
// The first candidate is attempted immediately. Every subsequent candidate is
// attempted interval after the previous one was started, unless an earlier
// attempt fails before that, in which case the next candidate is attempted
// right away. An interval of 0 starts all candidates at once. A negative
// interval is rejected. The first attempt that completes an SRT handshake wins;
// all other attempts are canceled and their sockets are closed before
// DialCandidates returns.
//
// A connection is only considered successful after the SRT handshake has
// completed. Sending a UDP packet or opening a socket alone is not enough.
//
// In case of an error the returned Conn is nil and the error is non-nil.
func DialCandidates(network string, addresses []string, config Config, interval time.Duration) (Conn, error) {
	return DialCandidatesWithContext(context.Background(), network, addresses, config, interval)
}

// DialCandidatesWithContext behaves like DialCandidates but cancels all
// attempts and releases their sockets and goroutines as soon as ctx is done.
// In that case the returned error is ctx.Err().
//
// Example:
//
//	DialCandidatesWithContext(ctx, "srt", []string{"10.0.0.1:3000", "10.0.0.2:3000"}, DefaultConfig(), 250*time.Millisecond)
//
// In case of an error the returned Conn is nil and the error is non-nil.
func DialCandidatesWithContext(ctx context.Context, network string, addresses []string, config Config, interval time.Duration) (Conn, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context must not be nil")
	}

	if network != "srt" {
		return nil, fmt.Errorf("the network must be 'srt'")
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	if interval < 0 {
		return nil, fmt.Errorf("candidate interval must not be negative")
	}

	if len(addresses) == 0 {
		return nil, ErrNoCandidates
	}

	if config.Logger == nil {
		config.Logger = NewLogger(nil)
	}

	candidates, err := normalizeCandidateAddresses(addresses)
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	type attemptResult struct {
		index int
		conn  Conn
		err   error
	}

	// Every candidate gets its own cancellable context so that a single
	// attempt can be stopped without affecting the others. Canceling the
	// caller's context cancels all of them.
	attemptCtxs := make([]context.Context, len(candidates))
	cancels := make([]context.CancelFunc, len(candidates))
	for i := range candidates {
		attemptCtxs[i], cancels[i] = context.WithCancel(ctx)
	}

	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	results := make(chan attemptResult, len(candidates))
	failures := make([]*CandidateError, len(candidates))

	launched := 0
	inFlight := 0

	launch := func() {
		i := launched
		launched++
		inFlight++

		config.Logger.Print("dial:candidates", 0, 2, func() string {
			return fmt.Sprintf("attempting candidate %d/%d %s", i+1, len(candidates), candidates[i])
		})

		go func() {
			conn, err := DialWithContext(attemptCtxs[i], network, candidates[i], config)
			results <- attemptResult{
				index: i,
				conn:  conn,
				err:   err,
			}
		}()
	}

	// Releases every attempt that is still running, closes any connection they
	// might still have produced, and waits until their sockets and goroutines
	// are gone. This is used both after a winner has been found and when the
	// caller cancels. The attempt with the given index is not canceled, use -1
	// to cancel every attempt.
	drain := func(keep int) {
		for i, cancel := range cancels {
			if i != keep {
				cancel()
			}
		}

		for ; inFlight > 0; inFlight-- {
			result := <-results
			if result.conn != nil {
				result.conn.Close()
			}
		}
	}

	launch()

	// An interval of zero means that every candidate is attempted at once.
	for interval <= 0 && launched < len(candidates) {
		launch()
	}

	timer := time.NewTimer(interval)
	defer timer.Stop()

	var timerChan <-chan time.Time
	if interval > 0 && launched < len(candidates) {
		timerChan = timer.C
	}

	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		if launched < len(candidates) {
			timer.Reset(interval)
			timerChan = timer.C
		} else {
			timerChan = nil
		}
	}

	for {
		select {
		case result := <-results:
			inFlight--

			if result.err == nil {
				// A cancellation that arrives together with a successful
				// handshake wins, as it does for a single address.
				if err := ctx.Err(); err != nil {
					result.conn.Close()
					drain(-1)
					return nil, err
				}

				config.Logger.Print("dial:candidates", 0, 2, func() string {
					return fmt.Sprintf("candidate %d/%d %s completed the handshake, closing the other attempts", result.index+1, len(candidates), candidates[result.index])
				})

				// Cancel the remaining attempts, but leave the winning
				// context alone. Cancel the others before waiting so that
				// drain() does not have to wait for their timeouts.
				drain(result.index)

				return result.conn, nil
			}

			// The caller canceled while this attempt was failing. Report the
			// cancellation instead of starting yet another attempt.
			if err := ctx.Err(); err != nil {
				drain(-1)
				return nil, err
			}

			failures[result.index] = &CandidateError{
				Address: candidates[result.index],
				Err:     result.err,
			}

			config.Logger.Print("dial:candidates", 0, 2, func() string {
				return fmt.Sprintf("candidate %d/%d %s failed: %s", result.index+1, len(candidates), candidates[result.index], result.err)
			})

			// A definitive failure is a reason to try the next candidate
			// immediately rather than waiting for the interval to pass.
			if launched < len(candidates) {
				launch()
				resetTimer()
				continue
			}

			if inFlight == 0 {
				return nil, newCandidatesError(failures)
			}

		case <-timerChan:
			// Don't start a new attempt once the caller has given up. The
			// select may pick the timer even though both cases are ready.
			if err := ctx.Err(); err != nil {
				drain(-1)
				return nil, err
			}

			// The timer is one-shot. Launch the next candidate and arm it
			// again for the one after that.
			launch()
			resetTimer()

		case <-ctx.Done():
			drain(-1)

			return nil, ctx.Err()
		}
	}
}

// normalizeCandidateAddresses validates the candidate addresses, trims
// surrounding whitespace, and collapses duplicates while preserving the order
// in which the addresses were given.
func normalizeCandidateAddresses(addresses []string) ([]string, error) {
	unique := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))

	for i, address := range addresses {
		address = strings.TrimSpace(address)

		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("%w: candidate %d (%q): %v", ErrInvalidCandidate, i+1, address, err)
		}

		if len(host) == 0 {
			return nil, fmt.Errorf("%w: candidate %d (%q): missing host", ErrInvalidCandidate, i+1, address)
		}

		if len(port) == 0 {
			return nil, fmt.Errorf("%w: candidate %d (%q): missing port", ErrInvalidCandidate, i+1, address)
		}

		portNumber, err := strconv.ParseUint(port, 10, 16)
		if err != nil || portNumber == 0 {
			return nil, fmt.Errorf("%w: candidate %d (%q): %q is not a valid port", ErrInvalidCandidate, i+1, address, port)
		}

		if _, exists := seen[address]; exists {
			continue
		}

		seen[address] = struct{}{}
		unique = append(unique, address)
	}

	return unique, nil
}

// newCandidatesError assembles the error for a failed DialCandidates call in
// the order of the candidates.
func newCandidatesError(failures []*CandidateError) *CandidatesError {
	err := &CandidatesError{
		Attempts: make([]CandidateError, 0, len(failures)),
	}

	for _, failure := range failures {
		if failure == nil {
			continue
		}

		err.Attempts = append(err.Attempts, *failure)
	}

	return err
}
