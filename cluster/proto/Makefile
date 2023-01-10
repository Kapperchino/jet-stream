service.pb.go: cluster.proto
	protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    cluster.proto

force:
	rm -f cluster.pb.go
	make cluster.pb.go
