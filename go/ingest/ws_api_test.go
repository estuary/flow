package ingest

import (
	"fmt"
	"testing"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func testCSVSimple(t *testing.T, addr string) {
	var valid = []string{
		"i,s",
		// May optionally include trailing newline.
		"32,hi\n",
		"42,bye",
		// Handles multiple records in one message.
		"72,first\n87,second",
		"23,one\n24,two\n25,three\n",
		// Quotes.
		`"62","whoot"`,
	}

	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/testing/int-string", nil)
	require.NoError(t, err)

	var whoops, processed = runWebsocket(t, c, valid)
	require.Equal(t, "", whoops)
	require.Equal(t, 8, processed)
}

func testTSVSimple(t *testing.T, addr string) {
	var valid = []string{
		"i\ts",
		"32\thi\n",
		"42\tone\n52\ttwo\n62\tthree\n",
		"72\tbye",
	}

	var dialer = websocket.Dialer{
		Subprotocols: []string{wsTSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/testing/int-string", nil)
	require.NoError(t, err)

	var whoops, processed = runWebsocket(t, c, valid)
	require.Equal(t, "", whoops)
	require.Equal(t, 5, processed)
}

func testCSVCollectionNotFound(t *testing.T, addr string) {
	var valid = []string{
		"i,s",
	}

	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/not/found", nil)
	require.NoError(t, err)

	var whoops, _ = runWebsocket(t, c, valid)
	require.Equal(t, "'not/found' is not an ingestable collection", whoops)
}

func testCSVMalformed(t *testing.T, addr string) {
	var valid = []string{
		"i,s",
		"32,hi,extra",
	}

	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/testing/int-string", nil)
	require.NoError(t, err)

	var whoops, _ = runWebsocket(t, c, valid)
	require.Equal(t, "processing frame: row has 3 columns, but header had only 2", whoops)
}

func testCSVMissingRequired(t *testing.T, addr string) {
	var valid = []string{
		"i,s",
		"32",
	}

	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/testing/int-string", nil)
	require.NoError(t, err)

	var whoops, _ = runWebsocket(t, c, valid)
	require.Equal(t, "processing frame: row omits column 1 ('s'), which must exist", whoops)
}

func testCSVProjectionNotFound(t *testing.T, addr string) {
	var valid = []string{
		"i,sss",
	}

	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/testing/int-string", nil)
	require.NoError(t, err)

	var whoops, _ = runWebsocket(t, c, valid)
	require.Equal(t, `processing frame: collection "testing/int-string" has no projection "sss"`, whoops)
}

func testCSVConversionError(t *testing.T, addr string) {
	var valid = []string{
		"i,s",
		"32.32,hi",
	}

	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/testing/int-string", nil)
	require.NoError(t, err)

	var whoops, _ = runWebsocket(t, c, valid)
	require.Equal(t, `processing frame: failed to parse '32.32' (of column: i) into [integer]: strconv.ParseInt: parsing "32.32": invalid syntax`, whoops)
}

func testCSVConvertRequiredNullable(t *testing.T, addr string) {
	var valid = []string{
		"theKey,string,boolean,integer,number,object,array,null",
		"someKey,,,,,,,",
	}

	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/weird-types/required-nullable", nil)
	require.NoError(t, err)

	var whoops, processed = runWebsocket(t, c, valid)
	require.Equal(t, "", whoops)
	require.Equal(t, 1, processed)
}

func testCSVMissingMustExistNullable(t *testing.T, addr string) {
	var valid = []string{
		"theKey,string,boolean,integer,number,object,array,null",
		"someKey,,,",
	}

	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/weird-types/required-nullable", nil)
	require.NoError(t, err)

	var whoops, _ = runWebsocket(t, c, valid)
	require.Equal(t, "processing frame: row omits column 7 ('null'), which must exist", whoops)
}

func testCSVOptionalMultipleTypes(t *testing.T, addr string) {
	var rows = []string{
		"theKey,stringOrInt,intOrNum,boolOrString",
		"a,55,66,true",
		"b,\"77\",\"1.23\",\"false\"", // Even though these are quoted, they will not import as strings
		"c,a real string,0,moar strings",
		"d", // columns may be undefined (but not null)
	}
	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/weird-types/optional-multi-types", nil)
	require.NoError(t, err)

	var whoops, processed = runWebsocket(t, c, rows)
	require.Equal(t, "", whoops)
	require.Equal(t, 4, processed)
}

func testCSVValueFailsValidation(t *testing.T, addr string) {
	// 0 is parseable as an int, but the document will fail validation because it's not within the correct range
	var rows = []string{
		"theKey,intDifferentRanges",
		"a,0",
	}
	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/weird-types/optional-multi-types", nil)
	require.NoError(t, err)

	var whoops, _ = runWebsocket(t, c, rows)
	require.Contains(t, whoops, "document is invalid")
}

func testCSVHeaderMissingRequiredField(t *testing.T, addr string) {
	// header is missing theKey
	var rows = []string{
		"string,bool,int,number",
	}
	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/weird-types/optionals", nil)
	require.NoError(t, err)

	var whoops, _ = runWebsocket(t, c, rows)
	require.Equal(t, "processing frame: Header does not include any field that maps to the location: '/theKey', which is required to exist by the collection schema", whoops)
}

func testCSVNumOrIntOrNull(t *testing.T, addr string) {
	var rows = []string{
		"theKey,intOrNumOrNull",
		"a,55.55",
		"b,\"77\"",
		"c,\"\"",
		"d",
	}
	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/weird-types/optional-multi-types", nil)
	require.NoError(t, err)

	var whoops, processed = runWebsocket(t, c, rows)
	require.Equal(t, "", whoops)
	require.Equal(t, 4, processed)
}

func testCSVOptionalObjectsAndArrays(t *testing.T, addr string) {
	var rows = []string{
		"theKey,intOrObjectOrNull,boolOrArrayOrNull",
		"a,55,true",
		"b,\"77\",\"true\"",
		// These will be null values
		"c,\"\",\"\"",
		"d,,",
		"e",
	}
	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/weird-types/optional-multi-types", nil)
	require.NoError(t, err)

	var whoops, processed = runWebsocket(t, c, rows)
	require.Equal(t, "", whoops)
	require.Equal(t, 5, processed)
}

func testCSVUnsupportedArray(t *testing.T, addr string) {
	var rows = []string{
		"theKey,boolOrArrayOrNull",
		"a,[]",
	}
	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/weird-types/optional-multi-types", nil)
	require.NoError(t, err)

	var whoops, processed = runWebsocket(t, c, rows)
	var expectedErr = "processing frame: failed to parse '[]' (of column: boolOrArrayOrNull) into [array boolean null]: unspported type 'array'"
	require.Equal(t, expectedErr, whoops)
	require.Equal(t, 0, processed)
}

func testCSVUnsupportedObject(t *testing.T, addr string) {
	var rows = []string{
		"theKey,intOrObjectOrNull",
		"a,{}",
	}
	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/weird-types/optional-multi-types", nil)
	require.NoError(t, err)

	var whoops, processed = runWebsocket(t, c, rows)
	var expectedErr = "processing frame: failed to parse '{}' (of column: intOrObjectOrNull) into [integer null object]: unspported type 'object'"
	require.Equal(t, expectedErr, whoops)
	require.Equal(t, 0, processed)
}

func testCSVEmptyBody(t *testing.T, addr string) {
	var dialer = websocket.Dialer{
		Subprotocols: []string{wsCSVProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/testing/int-string", nil)
	require.NoError(t, err)

	var whoops, _ = runWebsocket(t, c, []string{})
	require.Equal(t, "client closed the connection without sending any documents", whoops)
}

func testCSVTypeConversions(t *testing.T) {
	var doExtractValueTest = func(types []string, input string, expectedValue interface{}, expectedError error) {
		var projection = pf.Projection{
			Ptr:   "/the/ptr",
			Field: "theField",
			Inference: &pf.Inference{
				Types: types,
			},
		}
		var mapping = newFieldMapping(&projection)
		var value, err = mapping.extractValue(input)

		if expectedValue == nil {
			require.Nil(t, value)
		} else {
			require.Equal(t, expectedValue, value)
		}
		if expectedError == nil {
			require.Nil(t, err)
		} else {
			require.Equal(t, expectedError, err)
		}
	}
	var allTypes = []string{"integer", "string", "object", "number", "array", "boolean", "null"}

	// empty input always gets parsed as an explicit null if null is in the list of allowed types
	doExtractValueTest(allTypes, "", nil, nil)
	doExtractValueTest(allTypes, "3", uint64(3), nil)
	doExtractValueTest(allTypes, "1.234", 1.234, nil)
	doExtractValueTest(allTypes, "true", true, nil)
	doExtractValueTest(allTypes, "canary", "canary", nil)

	doExtractValueTest([]string{"boolean"}, "", nil, fmt.Errorf("value cannot be null"))
	doExtractValueTest([]string{"string", "null"}, "", nil, nil)

	var typesBesidesNull = []string{"integer", "number", "array", "object", "boolean", "string"}
	// If null is not in the allowed types, but string is, then an empty input gets parsed to an
	// empty string
	doExtractValueTest(typesBesidesNull, "", "", nil)
	doExtractValueTest(typesBesidesNull, "asdf", "asdf", nil)
}

func testJSONSimple(t *testing.T, addr string) {
	var valid = []string{
		// May optionally include trailing newline.
		`{"i": 32, "s": "hi"}` + "\n",
		`{"i": 42, "s": "bye"}`,
		// Handles multiple records in one message, separate by whitespace.
		// Trailing whitespace doesn't break anything.
		`{"i": 72, "s": "first"}` + "\n" + `{"i": 87, "s": "second"}`,
		`{"i": 23, "s": "one"}   {"i": 24, "s": "two"} ` + "\n" + ` {"i": 25, "s": "three"}  `,
	}

	var dialer = websocket.Dialer{
		Subprotocols: []string{wsJSONProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/testing/int-string", nil)
	require.NoError(t, err)

	var whoops, processed = runWebsocket(t, c, valid)
	require.Equal(t, "", whoops)
	require.Equal(t, 7, processed)
}

func testJSONInvalidSchema(t *testing.T, addr string) {
	var valid = []string{
		`{"missing": "required"}`,
	}

	var dialer = websocket.Dialer{
		Subprotocols: []string{wsJSONProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/testing/int-string", nil)
	require.NoError(t, err)

	var whoops, _ = runWebsocket(t, c, valid)
	require.Regexp(t, "ingestion of collection.*document is invalid: .*", whoops)
}

func testJSONMalformed(t *testing.T, addr string) {
	var valid = []string{
		`{"i": 32, "s": "hi"}` + "\n",
		`{"i": 42, "s":`,
	}

	var dialer = websocket.Dialer{
		Subprotocols: []string{wsJSONProtocol},
	}
	var c, _, err = dialer.Dial("ws://"+addr+"/ingest/testing/int-string", nil)
	require.NoError(t, err)

	var whoops, _ = runWebsocket(t, c, valid)
	require.Equal(t, "processing frame: unexpected EOF", whoops)
}

func runWebsocket(t *testing.T, c *websocket.Conn, msgs []string) (string, int) {
	// Write all messages and initiate close.
	for _, msg := range msgs {
		require.NoError(t, c.WriteMessage(websocket.TextMessage, []byte(msg)))
	}
	var frame = websocket.FormatCloseMessage(websocket.CloseNoStatusReceived, "")
	require.NoError(t, c.WriteControl(websocket.CloseMessage, frame, time.Time{}))

	var whoops string
	var processed int

	var message struct {
		// Success fields.
		Offsets   pb.Offsets
		Processed int
		Etcd      pb.Header_Etcd

		// Error fields.
		ApproximateFrame int
		Error            string
	}
	for {
		var err = c.ReadJSON(&message)
		if err != nil {
			if whoops == "" {
				require.True(t, websocket.IsCloseError(err, websocket.CloseNormalClosure), err)
			} else {
				require.True(t, websocket.IsCloseError(err, websocket.CloseProtocolError), err)
			}
			return whoops, processed
		}

		if message.Error != "" {
			whoops = message.Error
		} else {
			require.NotEmpty(t, message.Offsets)
			require.NotZero(t, message.Etcd.Revision)
			require.NotZero(t, message.Processed)
			processed = message.Processed
		}
	}
}
