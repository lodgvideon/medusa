package imap

import (
	"context"
	"errors"
	"testing"

	"github.com/lodgvideon/medusa/cluster"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/transport"
)

// fakeTransport lets us drive the send helpers' response-handling branches
// (unexpected type, transport error) without a real peer.
type fakeTransport struct {
	respType medusav1.MessageType
	err      error
}

func (f *fakeTransport) Addr() string                   { return "fake" }
func (f *fakeTransport) Listen(transport.Handler) error { return nil }
func (f *fakeTransport) Close() error                   { return nil }
func (f *fakeTransport) Send(_ context.Context, _ string, _ medusav1.MessageType, _, dst []byte) (medusav1.MessageType, []byte, error) {
	if f.err != nil {
		return 0, dst, f.err
	}
	return f.respType, dst[:0], nil
}

func svcWith(tr transport.Transport) *Service {
	mem := cluster.New(cluster.Member{ID: "self", Addr: "self"}, tr)
	return NewService(mem, tr)
}

func TestSendGetRejectsUnexpectedType(t *testing.T) {
	svc := svcWith(&fakeTransport{respType: medusav1.MessageType_MESSAGE_TYPE_PUT_RESPONSE})
	if _, _, err := svc.sendGet(context.Background(), "peer", "m", []byte("k")); err == nil {
		t.Fatal("want error on unexpected get response type")
	}
}

func TestSendPutRejectsUnexpectedType(t *testing.T) {
	svc := svcWith(&fakeTransport{respType: medusav1.MessageType_MESSAGE_TYPE_GET_RESPONSE})
	if err := svc.sendPut(context.Background(), "peer", "m", []byte("k"), []byte("v"), 0, false); err == nil {
		t.Fatal("want error on unexpected put response type")
	}
}

func TestSendRemoveRejectsUnexpectedType(t *testing.T) {
	svc := svcWith(&fakeTransport{respType: medusav1.MessageType_MESSAGE_TYPE_GET_RESPONSE})
	if _, err := svc.sendRemove(context.Background(), "peer", "m", []byte("k"), false); err == nil {
		t.Fatal("want error on unexpected remove response type")
	}
}

func TestSendHelpersPropagateTransportError(t *testing.T) {
	boom := errors.New("transport down")
	svc := svcWith(&fakeTransport{err: boom})
	ctx := context.Background()

	if _, _, err := svc.sendGet(ctx, "peer", "m", []byte("k")); !errors.Is(err, boom) {
		t.Errorf("sendGet err = %v, want boom", err)
	}
	if err := svc.sendPut(ctx, "peer", "m", []byte("k"), []byte("v"), 0, false); !errors.Is(err, boom) {
		t.Errorf("sendPut err = %v, want boom", err)
	}
	if _, err := svc.sendRemove(ctx, "peer", "m", []byte("k"), false); !errors.Is(err, boom) {
		t.Errorf("sendRemove err = %v, want boom", err)
	}
}

func TestHandleRejectsCorruptPayload(t *testing.T) {
	svc := svcWith(&fakeTransport{})
	bad := []byte{0xff, 0xff, 0xff} // not a valid protobuf message

	for _, mt := range []medusav1.MessageType{
		medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST,
		medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST,
		medusav1.MessageType_MESSAGE_TYPE_REMOVE_REQUEST,
	} {
		if _, _, err := svc.Handle(mt, bad, nil); err == nil {
			t.Errorf("Handle(%v, corrupt) err = nil, want decode error", mt)
		}
	}
}
