package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	hdr := make([]byte, headerSize)
	payload := []byte("hello-frame")

	if err := writeFrame(&buf, hdr, medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST, payload); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}

	gotType, gotPayload, err := readFrame(&buf, make([]byte, headerSize), nil)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if gotType != medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST {
		t.Errorf("type = %v, want PUT_REQUEST", gotType)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Errorf("payload = %q, want %q", gotPayload, payload)
	}
}

func TestReadFrameTooLarge(t *testing.T) {
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[:], MaxFrameSize+1)
	r := bytes.NewReader(hdr[:])

	_, _, err := readFrame(r, make([]byte, headerSize), nil)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrameTruncatedHeader(t *testing.T) {
	r := bytes.NewReader([]byte{0x00, 0x01}) // fewer than headerSize bytes
	_, _, err := readFrame(r, make([]byte, headerSize), nil)
	if err == nil {
		t.Fatal("expected an error on truncated header")
	}
}

func TestReadFrameTruncatedPayload(t *testing.T) {
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[:], 10)                 // claims 10 bytes...
	r := bytes.NewReader(append(hdr[:], []byte("abc")...)) // ...but only 3 follow
	_, _, err := readFrame(r, make([]byte, headerSize), nil)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
	}
}
