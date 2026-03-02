// Implements a lightweight custom binary RPC framework over TCP.
// Wire frame format (per message):
// Payload length = 4 bytes
// Message type = 1 byte
// Payload = N bytes (gob-encoded struct, varies by message type)
//
// The length field encodes the size of the payload only.
package rpc
