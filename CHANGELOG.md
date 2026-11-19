# Changelog

## [Unreleased]

### Added

- Receive deadline audit. Setting `Config.RecvDeadlineAudit` to a queue
  capacity enables one `RecvDeadlineEvent` per received data packet on the
  channel returned by `Conn.RecvDeadlineEvents`, reporting the packet
  sequence number, media timestamp, scheduled TSBPD delivery time, and
  whether the packet was delivered on time, late, or dropped. The queue is
  bounded; events that don't fit are discarded and counted by
  `Conn.RecvDeadlineEventsDropped`.

### Fixed

- Honor received DROPREQ ranges in live mode so the receiver stops requesting
  packets the sender has dropped and can acknowledge past those gaps without
  waiting for a later packet's playout time. Keep already received packets
  scheduled for delivery and bound pending drop metadata.
- Remove accepted DROPREQ ranges from pending reorder losses and exclude them
  when registering later gaps, preserving the TTL of other missing packets.
