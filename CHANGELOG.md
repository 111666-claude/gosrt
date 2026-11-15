# Changelog

## [Unreleased]

### Added

- Add `DialCandidates` and `DialCandidatesWithContext` to dial a list of
  candidate addresses (for example a primary and a backup SRT entry point).
  The first candidate is attempted immediately and every further candidate
  after a configurable interval, or right away when an earlier attempt fails.
  The result is the first connection that completed the SRT handshake; the
  remaining attempts are canceled and their sockets closed. Invalid
  addresses, duplicate addresses, and the failure of all candidates are
  reported as distinct errors that name each candidate.

### Fixed

- Honor received DROPREQ ranges in live mode so the receiver stops requesting
  packets the sender has dropped and can acknowledge past those gaps without
  waiting for a later packet's playout time. Keep already received packets
  scheduled for delivery and bound pending drop metadata.
- Remove accepted DROPREQ ranges from pending reorder losses and exclude them
  when registering later gaps, preserving the TTL of other missing packets.
