//go:generate protoc --proto_path=$GOPATH/src:$GOPATH/src/github.com/gogo/protobuf/protobuf:. --go_out=plugins=grpc:. protocol.proto

package block
