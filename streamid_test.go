package srt

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamIdParse(t *testing.T) {
	sid, err := ParseStreamId("#!::m=publish,r=live/match1,u=camera1,pass=s3cret,t=stream,s=abc123")
	require.NoError(t, err)

	assert.Equal(t, ModePublish, sid.Mode)
	assert.Equal(t, "live/match1", sid.Resource)
	assert.Equal(t, "camera1", sid.User)
	assert.Equal(t, "s3cret", sid.Password)
	assert.Equal(t, "stream", sid.Type)
	assert.Equal(t, "abc123", sid.SessionId)
	assert.Empty(t, sid.Custom)
}

func TestStreamIdParseWithoutPrefix(t *testing.T) {
	sid, err := ParseStreamId("m=request,r=live/match2")
	require.NoError(t, err)

	assert.Equal(t, ModeRequest, sid.Mode)
	assert.Equal(t, "live/match2", sid.Resource)
}

func TestStreamIdParseCustomKeys(t *testing.T) {
	sid, err := ParseStreamId("#!::m=publish,r=a,x-bitrate=4500,x_codec=h264")
	require.NoError(t, err)

	assert.Equal(t, map[string]string{"x-bitrate": "4500", "x_codec": "h264"}, sid.Custom)
}

func TestStreamIdRoundTrip(t *testing.T) {
	for _, raw := range []string{
		"#!::m=publish,r=live/match1",
		"#!::m=request,r=live/match1,u=viewer,pass=topsecret",
		"#!::m=publish,r=a/u=1,t=stream,s=sess,x-b=2,x-a=1",
		"#!::r=only-resource",
	} {
		sid, err := ParseStreamId(raw)
		require.NoError(t, err, raw)

		encoded := sid.String()

		sid2, err := ParseStreamId(encoded)
		require.NoError(t, err, encoded)

		assert.Equal(t, sid, sid2, "round trip of %q via %q", raw, encoded)
	}
}

func TestStreamIdEncodeOrder(t *testing.T) {
	sid := &StreamId{
		Mode:      ModePublish,
		Resource:  "r",
		User:      "u",
		Password:  "p",
		Type:      "t",
		SessionId: "s",
		Custom:    map[string]string{"z": "1", "a": "2"},
	}

	assert.Equal(t, "#!::m=publish,r=r,u=u,pass=p,t=t,s=s,a=2,z=1", sid.String())
}

func TestStreamIdParseInvalid(t *testing.T) {
	for _, raw := range []string{
		"",                        // empty
		"#!::",                    // only the marker
		"m=publish,m=publish",     // duplicate standard key
		"m=publish,r=a,r=b",       // duplicate resource
		"m=publish,x=1,x=2",       // duplicate custom key
		"m=publish,novalue",       // missing '='
		"m=publish,=value",        // empty key
		"m=stream,r=a",            // unknown mode
		"m=publish,r=a,bad key=1", // invalid key character
		"m=publish,r=a\x07b",      // control character in value
	} {
		_, err := ParseStreamId(raw)
		assert.Error(t, err, raw)
	}

	// Empty values are allowed
	sid, err := ParseStreamId("#!::m=publish,r=a,u=,pass=")
	require.NoError(t, err)
	assert.Equal(t, "", sid.User)
	assert.Equal(t, "", sid.Password)
}

func TestStreamIdValidate(t *testing.T) {
	sid := &StreamId{Mode: ModePublish, Resource: "a"}
	require.NoError(t, sid.Validate())

	sid = &StreamId{Mode: "bogus"}
	require.Error(t, sid.Validate())

	sid = &StreamId{Resource: "a,b"}
	require.Error(t, sid.Validate())

	sid = &StreamId{Custom: map[string]string{"m": "publish"}}
	require.Error(t, sid.Validate())

	sid = &StreamId{Custom: map[string]string{"ok": "v"}}
	require.NoError(t, sid.Validate())
}

func TestStreamIdRedacted(t *testing.T) {
	sid, err := ParseStreamId("#!::m=publish,r=live/match1,u=camera1,pass=s3cret,token=abc")
	require.NoError(t, err)

	redacted := sid.Redacted()

	assert.NotContains(t, redacted, "s3cret")
	assert.NotContains(t, redacted, "abc")
	assert.Contains(t, redacted, "pass=***")
	assert.Contains(t, redacted, "token=***")
	assert.Contains(t, redacted, "u=camera1")
	assert.Contains(t, redacted, "r=live/match1")

	// The original must be unchanged
	assert.Equal(t, "s3cret", sid.Password)
}

func TestRedactStreamId(t *testing.T) {
	// Structured streamid
	assert.NotContains(t, RedactStreamId("#!::m=publish,r=a,pass=s3cret"), "s3cret")

	// Non-structured streamid with a credential-looking key
	assert.Equal(t, "garbage,pass=***", RedactStreamId("garbage,pass=s3cret"))
	assert.Equal(t, "#!::pass=***", RedactStreamId("#!::pass=s3cret"))

	// Streamids without credentials pass through
	assert.Equal(t, "foobar", RedactStreamId("foobar"))
	assert.Equal(t, "#!::m=publish,r=a", RedactStreamId("#!::m=publish,r=a"))
}
