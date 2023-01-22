rafradmin.pb.go: raftadmin.proto
	protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    raftadmin.proto

force:
	rm -f rafradmin.pb.go
	make raftadmin.pb.go
