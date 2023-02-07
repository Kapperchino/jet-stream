transport.pb.go: transport.proto
	protoc \
    --go_out=. --plugin protoc-gen-go="${GOBIN}/protoc-gen-go" \
    --go-grpc_out=. --plugin protoc-gen-go-grpc="${GOBIN}/protoc-gen-go-grpc" \
    --go-vtproto_out=. --plugin protoc-gen-go-vtproto="${GOBIN}/protoc-gen-go-vtproto" \
    --go-vtproto_opt=features=marshal+unmarshal+size+pool \
    transport.proto

force:
	rm -f transport.pb.go
	make transport.pb.go
