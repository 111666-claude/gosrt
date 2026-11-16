# Changelog

## [Unreleased]

### Added

- `Conn.SubscribeStats` starts a cancellable periodic statistics
  subscription. Each subscription samples the accumulated, interval and
  instantaneous statistics on its own goroutine with its own interval and
  bounded buffer, delivers them together with the wall-clock sampling time,
  drops the oldest snapshot when a consumer falls behind, and stops when it
  is cancelled or the connection is closed.

### Fixed

- Honor received DROPREQ ranges in live mode so the receiver stops requesting
  packets the sender has dropped and can acknowledge past those gaps without
  waiting for a later packet's playout time. Keep already received packets
  scheduled for delivery and bound pending drop metadata.
- Remove accepted DROPREQ ranges from pending reorder losses and exclude them
  when registering later gaps, preserving the TTL of other missing packets.
