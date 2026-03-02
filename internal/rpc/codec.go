package rpc

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"net"
)

const headerSize = 5 // 4 (length) + 1 (type)

// Encode serialises payload using gob and writes a framed message to conn.
func Encode(conn net.Conn, msgType uint8, payload any) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(payload); err != nil {
		return fmt.Errorf("rpc encode: gob encode: %w", err)
	}

	encoded := buf.Bytes()
	header := make([]byte, headerSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(encoded)))
	header[4] = msgType

	_, err := conn.Write(append(header, encoded...))
	return err
}

// Decode reads one framed message from conn.
// Returns the message type byte and the raw gob-encoded payload bytes.
func Decode(conn net.Conn) (msgType uint8, payload []byte, err error) {
	header := make([]byte, headerSize)
	if _, err = io.ReadFull(conn, header); err != nil {
		return 0, nil, fmt.Errorf("rpc decode: read header: %w", err)
	}

	length := binary.BigEndian.Uint32(header[0:4])
	msgType = header[4]

	payload = make([]byte, length)
	if _, err = io.ReadFull(conn, payload); err != nil {
		return 0, nil, fmt.Errorf("rpc decode: read payload: %w", err)
	}
	return msgType, payload, nil
}

// DecodePayload deserialises raw gob bytes into dst (must be a pointer).
func DecodePayload(data []byte, dst any) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("rpc decode payload: %w", err)
	}
	return nil
}
