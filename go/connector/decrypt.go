package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"

	"github.com/estuary/flow/go/pkgbin"
)

// DecryptConfig decrypts a `sops`-protected configuration document.
// If |config| isn't a sops document, an unmodified copy is returned.
// Otherwise the decrypted form of the document is returned.
// DecryptConfig takes pains to only create a single allocated copy of the
// decrypted document, which is then returned to the caller. The caller should
// ZeroBytes() of the document as soon as it's no longer needed.
func DecryptConfig(ctx context.Context, config json.RawMessage) (json.RawMessage, error) {
	// |envelope| is expected structure of a `sops`-encrypted document.
	var envelope struct {
		Sops *struct {
			EncryptedSuffix string `json:"encrypted_suffix"`
		} `json:"sops"`
	}
	if err := json.Unmarshal(config, &envelope); err != nil {
		return nil, fmt.Errorf("go.estuary.dev/E127: decoding `sops` stanza: %w", err)
	}

	// If this isn't a `sops` document, return a copy of it unmodified.
	if envelope.Sops == nil {
		return append(json.RawMessage(nil), config...), nil
	}

	decrypted, err := decryptCmd(ctx, config, "sops",
		"--decrypt",
		"--input-type", "json",
		"--output-type", "json",
		"/dev/stdin",
	)
	if err != nil {
		return nil, fmt.Errorf("go.estuary.dev/E128: decrypting `sops` document: %w", err)
	}

	// If the envelope doesn't include an encrypted suffix, we're finished.
	if envelope.Sops.EncryptedSuffix == "" {
		return decrypted, nil
	}
	// We must re-write the document to remove the encrypted suffix.
	// |decrypted| is now only used as input for this stripping process.
	defer ZeroBytes(decrypted)

	// We'll use `jq` to do the re-writing. This avoids allocating and parsing
	// values in our own heap, and is also succinct.
	// See: https://jqplay.org/s/sQunN3Qc4s
	stripped, err := decryptCmd(ctx, decrypted, "jq",
		"-c",
		"walk(if type == \"object\" then with_entries(. + {key: .key | "+
			"rtrimstr(\""+envelope.Sops.EncryptedSuffix+"\")}) else . end)",
	)
	if err != nil {
		return nil, fmt.Errorf("go.estuary.dev/E128: stripping encrypted suffix %q from document: %w",
			envelope.Sops.EncryptedSuffix, err)
	}

	return stripped, nil
}

// ZeroBytes zeros the allocated capacity of the []byte slice.
func ZeroBytes(b []byte) {
	b = b[:cap(b)]
	for i := 0; i != len(b); i++ {
		b[i] = 0
	}
}

func decryptCmd(ctx context.Context, input []byte, args ...string) ([]byte, error) {
	var stdout, stderr bytes.Buffer

	// Initialize a buffer to read a decrypted result. The decryption cannot be
	// larger than the encrypted source |input|, and the pre-allocation prevents
	// additional allocations and copies from floating around the heap.
	stdout.Grow(len(input))

	var path, err = pkgbin.Locate(args[0])
	if err != nil {
		return nil, fmt.Errorf("go.estuary.dev/E129: finding %q binary: %w", args[0], err)
	}
	var cmd = exec.CommandContext(ctx, path, args[1:]...)

	// Run the command, passing in |input| and gathering output.
	// This is equivalent to cmd.Output(), but uses our pre-allocated output buffer.
	cmd.Stdin = bytes.NewReader(input)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()

	if stdout.Len() > len(input) {
		panic("go.estuary.dev/E130: decrypted output overflows pre-allocated buffer")
	}

	if err != nil {
		ZeroBytes(stdout.Bytes())
		return nil, fmt.Errorf("go.estuary.dev/E131: %w: %s", err, stderr.String())
	}

	return stdout.Bytes(), nil
}
