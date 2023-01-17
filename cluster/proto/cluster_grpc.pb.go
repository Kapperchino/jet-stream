// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.12
// source: cluster.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ClusterMetaServiceClient is the client API for ClusterMetaService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ClusterMetaServiceClient interface {
	GetPeers(ctx context.Context, in *GetPeersRequest, opts ...grpc.CallOption) (*GetPeersResponse, error)
	GetClusterInfo(ctx context.Context, in *GetClusterInfoRequest, opts ...grpc.CallOption) (*GetClusterInfoResponse, error)
	GetShardInfo(ctx context.Context, in *GetShardInfoRequest, opts ...grpc.CallOption) (*GetShardInfoResponse, error)
}

type clusterMetaServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewClusterMetaServiceClient(cc grpc.ClientConnInterface) ClusterMetaServiceClient {
	return &clusterMetaServiceClient{cc}
}

func (c *clusterMetaServiceClient) GetPeers(ctx context.Context, in *GetPeersRequest, opts ...grpc.CallOption) (*GetPeersResponse, error) {
	out := new(GetPeersResponse)
	err := c.cc.Invoke(ctx, "/ClusterMetaService/GetPeers", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *clusterMetaServiceClient) GetClusterInfo(ctx context.Context, in *GetClusterInfoRequest, opts ...grpc.CallOption) (*GetClusterInfoResponse, error) {
	out := new(GetClusterInfoResponse)
	err := c.cc.Invoke(ctx, "/ClusterMetaService/GetClusterInfo", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *clusterMetaServiceClient) GetShardInfo(ctx context.Context, in *GetShardInfoRequest, opts ...grpc.CallOption) (*GetShardInfoResponse, error) {
	out := new(GetShardInfoResponse)
	err := c.cc.Invoke(ctx, "/ClusterMetaService/GetShardInfo", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ClusterMetaServiceServer is the server API for ClusterMetaService service.
// All implementations must embed UnimplementedClusterMetaServiceServer
// for forward compatibility
type ClusterMetaServiceServer interface {
	GetPeers(context.Context, *GetPeersRequest) (*GetPeersResponse, error)
	GetClusterInfo(context.Context, *GetClusterInfoRequest) (*GetClusterInfoResponse, error)
	GetShardInfo(context.Context, *GetShardInfoRequest) (*GetShardInfoResponse, error)
	mustEmbedUnimplementedClusterMetaServiceServer()
}

// UnimplementedClusterMetaServiceServer must be embedded to have forward compatible implementations.
type UnimplementedClusterMetaServiceServer struct {
}

func (UnimplementedClusterMetaServiceServer) GetPeers(context.Context, *GetPeersRequest) (*GetPeersResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetPeers not implemented")
}
func (UnimplementedClusterMetaServiceServer) GetClusterInfo(context.Context, *GetClusterInfoRequest) (*GetClusterInfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetClusterInfo not implemented")
}
func (UnimplementedClusterMetaServiceServer) GetShardInfo(context.Context, *GetShardInfoRequest) (*GetShardInfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetShardInfo not implemented")
}
func (UnimplementedClusterMetaServiceServer) mustEmbedUnimplementedClusterMetaServiceServer() {}

// UnsafeClusterMetaServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ClusterMetaServiceServer will
// result in compilation errors.
type UnsafeClusterMetaServiceServer interface {
	mustEmbedUnimplementedClusterMetaServiceServer()
}

func RegisterClusterMetaServiceServer(s grpc.ServiceRegistrar, srv ClusterMetaServiceServer) {
	s.RegisterService(&ClusterMetaService_ServiceDesc, srv)
}

func _ClusterMetaService_GetPeers_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetPeersRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterMetaServiceServer).GetPeers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/ClusterMetaService/GetPeers",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterMetaServiceServer).GetPeers(ctx, req.(*GetPeersRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ClusterMetaService_GetClusterInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetClusterInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterMetaServiceServer).GetClusterInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/ClusterMetaService/GetClusterInfo",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterMetaServiceServer).GetClusterInfo(ctx, req.(*GetClusterInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ClusterMetaService_GetShardInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetShardInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterMetaServiceServer).GetShardInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/ClusterMetaService/GetShardInfo",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterMetaServiceServer).GetShardInfo(ctx, req.(*GetShardInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ClusterMetaService_ServiceDesc is the grpc.ServiceDesc for ClusterMetaService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ClusterMetaService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "ClusterMetaService",
	HandlerType: (*ClusterMetaServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetPeers",
			Handler:    _ClusterMetaService_GetPeers_Handler,
		},
		{
			MethodName: "GetClusterInfo",
			Handler:    _ClusterMetaService_GetClusterInfo_Handler,
		},
		{
			MethodName: "GetShardInfo",
			Handler:    _ClusterMetaService_GetShardInfo_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "cluster.proto",
}
