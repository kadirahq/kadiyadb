//go:generate protoc -I $PWD -I $GOPATH/src --gofast_out=. $PWD/protocol.proto

// When using --gogo* instead of --gofast_out the API for using proto3 feature
// `oneof` changes. So here we use --gofast and at places we don't need `oneof`
// --gogo* will be used.

package server
