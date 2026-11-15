package srt

import (
	"fmt"
	"sort"
	"strings"
)

// streamIdPrefix is the marker defined by the SRT Access Control guidelines
// that announces a structured, key=value based streamid.
const streamIdPrefix = "#!::"

// StreamIdMode is the mode of a structured streamid (the "m" key).
type StreamIdMode string

const (
	// ModePublish means the connection sends data to the server.
	ModePublish StreamIdMode = "publish"
	// ModeRequest means the connection wants to receive data from the server.
	ModeRequest StreamIdMode = "request"
	// ModeBidirectional means the connection sends and receives data.
	ModeBidirectional StreamIdMode = "bidirectional"
)

// StreamId is the structured representation of a SRT streamid following
// the SRT Access Control guidelines:
//
//	#!::m=publish,r=live/match1,u=camera1,pass=secret
//
// The leading "#!::" marker is optional when parsing and always emitted
// when encoding. Unknown keys are preserved in Custom.
type StreamId struct {
	Mode      StreamIdMode      // "m" key: publish, request, or bidirectional
	Resource  string            // "r" key: the path of the resource
	User      string            // "u" key: optional user name
	Password  string            // "pass" key: optional credential
	Type      string            // "t" key: optional media type
	SessionId string            // "s" key: optional session id
	Custom    map[string]string // all other keys
}

// sensitiveStreamIdKeys are key names whose values are considered
// credentials and must not appear in logs.
var sensitiveStreamIdKeys = map[string]bool{
	"pass":       true,
	"password":   true,
	"passphrase": true,
	"token":      true,
	"secret":     true,
	"key":        true,
}

// ParseStreamId parses a structured streamid. It accepts an optional leading
// "#!::" marker. Fields must be comma-separated key=value pairs. Keys must
// be non-empty and may contain letters, digits, '_', and '-'. Duplicate keys
// and unknown modes are rejected. Values must not contain control characters.
func ParseStreamId(raw string) (*StreamId, error) {
	s := strings.TrimPrefix(raw, streamIdPrefix)
	if len(s) == 0 {
		return nil, fmt.Errorf("streamid: empty")
	}

	sid := &StreamId{}
	seen := make(map[string]bool)

	for _, field := range strings.Split(s, ",") {
		key, value, found := strings.Cut(field, "=")
		if !found {
			return nil, fmt.Errorf("streamid: invalid field %q, expecting key=value", field)
		}

		if err := validateStreamIdKey(key); err != nil {
			return nil, err
		}

		if seen[key] {
			return nil, fmt.Errorf("streamid: duplicate key %q", key)
		}
		seen[key] = true

		if err := validateStreamIdValue(value); err != nil {
			return nil, err
		}

		switch key {
		case "m":
			switch StreamIdMode(value) {
			case ModePublish, ModeRequest, ModeBidirectional:
				sid.Mode = StreamIdMode(value)
			default:
				return nil, fmt.Errorf("streamid: unsupported mode %q", value)
			}
		case "r":
			sid.Resource = value
		case "u":
			sid.User = value
		case "pass":
			sid.Password = value
		case "t":
			sid.Type = value
		case "s":
			sid.SessionId = value
		default:
			if sid.Custom == nil {
				sid.Custom = make(map[string]string)
			}
			sid.Custom[key] = value
		}
	}

	return sid, nil
}

func validateStreamIdKey(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("streamid: empty key")
	}

	for _, r := range key {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' && r != '-' {
			return fmt.Errorf("streamid: invalid character %q in key %q", r, key)
		}
	}

	return nil
}

func validateStreamIdValue(value string) error {
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("streamid: control character in value %q", value)
		}
		if r == ',' {
			return fmt.Errorf("streamid: value %q must not contain a comma", value)
		}
	}

	return nil
}

// Validate checks that the StreamId can be encoded into a valid structured
// streamid string.
func (s *StreamId) Validate() error {
	switch s.Mode {
	case "", ModePublish, ModeRequest, ModeBidirectional:
	default:
		return fmt.Errorf("streamid: unsupported mode %q", s.Mode)
	}

	fields := []struct {
		key   string
		value string
	}{
		{"r", s.Resource},
		{"u", s.User},
		{"pass", s.Password},
		{"t", s.Type},
		{"s", s.SessionId},
	}

	for _, f := range fields {
		if err := validateStreamIdValue(f.value); err != nil {
			return err
		}
	}

	for key, value := range s.Custom {
		switch key {
		case "m", "r", "u", "pass", "t", "s":
			return fmt.Errorf("streamid: duplicate key %q", key)
		}
		if err := validateStreamIdKey(key); err != nil {
			return err
		}
		if err := validateStreamIdValue(value); err != nil {
			return err
		}
	}

	return nil
}

// String encodes the StreamId into its structured string form. The standard
// keys are emitted first in a fixed order, custom keys follow sorted by name.
// Invalid values (e.g. containing a comma) are encoded as-is; use Validate
// to check the StreamId beforehand.
func (s *StreamId) String() string {
	var b strings.Builder

	b.WriteString(streamIdPrefix)

	first := true
	write := func(key, value string) {
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(value)
	}

	if len(s.Mode) != 0 {
		write("m", string(s.Mode))
	}
	if len(s.Resource) != 0 {
		write("r", s.Resource)
	}
	if len(s.User) != 0 {
		write("u", s.User)
	}
	if len(s.Password) != 0 {
		write("pass", s.Password)
	}
	if len(s.Type) != 0 {
		write("t", s.Type)
	}
	if len(s.SessionId) != 0 {
		write("s", s.SessionId)
	}

	keys := make([]string, 0, len(s.Custom))
	for key := range s.Custom {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		write(key, s.Custom[key])
	}

	return b.String()
}

// Redacted returns the encoded streamid with the password (and any custom
// key that looks like a credential) masked. Use this for logging.
func (s *StreamId) Redacted() string {
	c := *s

	if len(c.Password) != 0 {
		c.Password = "***"
	}

	if len(c.Custom) != 0 {
		custom := make(map[string]string, len(c.Custom))
		for key, value := range c.Custom {
			if sensitiveStreamIdKeys[strings.ToLower(key)] && len(value) != 0 {
				value = "***"
			}
			custom[key] = value
		}
		c.Custom = custom
	}

	return c.String()
}

// RedactStreamId returns the given raw streamid with all credential values
// masked. It is safe to use on any streamid, structured or not, and should
// be used whenever a streamid is written to a log.
func RedactStreamId(raw string) string {
	if sid, err := ParseStreamId(raw); err == nil {
		return sid.Redacted()
	}

	// Best effort for non-structured streamids: mask the values of all
	// fields whose key looks like a credential.
	fields := strings.Split(raw, ",")
	for i, field := range fields {
		key, _, found := strings.Cut(field, "=")
		if !found {
			continue
		}
		if sensitiveStreamIdKeys[strings.ToLower(strings.TrimPrefix(key, streamIdPrefix))] {
			fields[i] = key + "=***"
		}
	}

	return strings.Join(fields, ",")
}
