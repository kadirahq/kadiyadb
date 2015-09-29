//go:generate protoc -I $PWD -I $GOPATH/src --gogoslick_out=. $PWD/protocol.proto
package server
