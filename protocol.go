//go:generate protoc -I $PWD -I $GOPATH/src -I $GOPATH/src/github.com/google/protobuf/src --gogoslick_out=. $PWD/protocol.proto
package kadiyadb
