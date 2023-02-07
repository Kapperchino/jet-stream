raftadmin.pb.go: raftadmin.proto
	protoc \
    --go_out=. --plugin protoc-gen-go="${GOBIN}/protoc-gen-go" \
    --go-grpc_out=. --plugin protoc-gen-go-grpc="${GOBIN}/protoc-gen-go-grpc" \
    --go-vtproto_out=. --plugin protoc-gen-go-vtproto="${GOBIN}/protoc-gen-go-vtproto" \
    --go-vtproto_opt=features=marshal+unmarshal+size+pool \
    raftadmin.proto

force:
	rm -f raftadmin.pb.go
	make raftadmin.pb.go
