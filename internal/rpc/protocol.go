// Package rpc implements a lightweight custom binary RPC framework over TCP.
//
// Wire frame format (per message):
//
//	┌──────────────────┬────────────────┬──────────────────────────┐
//	│ 4 bytes (uint32) │  1 byte (uint8)│  N bytes                 │
//	│  payload length  │  message type  │  gob-encoded payload     │
//	└──────────────────┴────────────────┴──────────────────────────┘
//
// The length field encodes the size of the payload only (not the header).
package rpc
