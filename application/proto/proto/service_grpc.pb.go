// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v3.21.12
// source: service.proto

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

const (
	MessageService_PublishMessages_FullMethodName     = "/message.MessageService/PublishMessages"
	MessageService_CreateConsumer_FullMethodName      = "/message.MessageService/CreateConsumer"
	MessageService_CreateConsumerGroup_FullMethodName = "/message.MessageService/CreateConsumerGroup"
	MessageService_GetConsumerGroups_FullMethodName   = "/message.MessageService/GetConsumerGroups"
	MessageService_Consume_FullMethodName             = "/message.MessageService/Consume"
	MessageService_CreateTopic_FullMethodName         = "/message.MessageService/CreateTopic"
	MessageService_AckConsume_FullMethodName          = "/message.MessageService/AckConsume"
	MessageService_GetMeta_FullMethodName             = "/message.MessageService/GetMeta"
)

// MessageServiceClient is the client API for MessageService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MessageServiceClient interface {
	PublishMessages(ctx context.Context, in *PublishMessageRequest, opts ...grpc.CallOption) (*PublishMessageResponse, error)
	CreateConsumer(ctx context.Context, in *CreateConsumerRequest, opts ...grpc.CallOption) (*CreateConsumerResponse, error)
	CreateConsumerGroup(ctx context.Context, in *CreateConsumerGroupRequest, opts ...grpc.CallOption) (*CreateConsumerGroupResponse, error)
	GetConsumerGroups(ctx context.Context, in *GetConsumerGroupsRequest, opts ...grpc.CallOption) (*GetConsumerGroupsResponse, error)
	Consume(ctx context.Context, in *ConsumeRequest, opts ...grpc.CallOption) (*ConsumeResponse, error)
	CreateTopic(ctx context.Context, in *CreateTopicRequest, opts ...grpc.CallOption) (*CreateTopicResponse, error)
	AckConsume(ctx context.Context, in *AckConsumeRequest, opts ...grpc.CallOption) (*AckConsumeResponse, error)
	GetMeta(ctx context.Context, in *GetMetaRequest, opts ...grpc.CallOption) (*GetMetaResponse, error)
}

type messageServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewMessageServiceClient(cc grpc.ClientConnInterface) MessageServiceClient {
	return &messageServiceClient{cc}
}

func (c *messageServiceClient) PublishMessages(ctx context.Context, in *PublishMessageRequest, opts ...grpc.CallOption) (*PublishMessageResponse, error) {
	out := new(PublishMessageResponse)
	err := c.cc.Invoke(ctx, MessageService_PublishMessages_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *messageServiceClient) CreateConsumer(ctx context.Context, in *CreateConsumerRequest, opts ...grpc.CallOption) (*CreateConsumerResponse, error) {
	out := new(CreateConsumerResponse)
	err := c.cc.Invoke(ctx, MessageService_CreateConsumer_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *messageServiceClient) CreateConsumerGroup(ctx context.Context, in *CreateConsumerGroupRequest, opts ...grpc.CallOption) (*CreateConsumerGroupResponse, error) {
	out := new(CreateConsumerGroupResponse)
	err := c.cc.Invoke(ctx, MessageService_CreateConsumerGroup_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *messageServiceClient) GetConsumerGroups(ctx context.Context, in *GetConsumerGroupsRequest, opts ...grpc.CallOption) (*GetConsumerGroupsResponse, error) {
	out := new(GetConsumerGroupsResponse)
	err := c.cc.Invoke(ctx, MessageService_GetConsumerGroups_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *messageServiceClient) Consume(ctx context.Context, in *ConsumeRequest, opts ...grpc.CallOption) (*ConsumeResponse, error) {
	out := new(ConsumeResponse)
	err := c.cc.Invoke(ctx, MessageService_Consume_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *messageServiceClient) CreateTopic(ctx context.Context, in *CreateTopicRequest, opts ...grpc.CallOption) (*CreateTopicResponse, error) {
	out := new(CreateTopicResponse)
	err := c.cc.Invoke(ctx, MessageService_CreateTopic_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *messageServiceClient) AckConsume(ctx context.Context, in *AckConsumeRequest, opts ...grpc.CallOption) (*AckConsumeResponse, error) {
	out := new(AckConsumeResponse)
	err := c.cc.Invoke(ctx, MessageService_AckConsume_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *messageServiceClient) GetMeta(ctx context.Context, in *GetMetaRequest, opts ...grpc.CallOption) (*GetMetaResponse, error) {
	out := new(GetMetaResponse)
	err := c.cc.Invoke(ctx, MessageService_GetMeta_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MessageServiceServer is the server API for MessageService service.
// All implementations must embed UnimplementedMessageServiceServer
// for forward compatibility
type MessageServiceServer interface {
	PublishMessages(context.Context, *PublishMessageRequest) (*PublishMessageResponse, error)
	CreateConsumer(context.Context, *CreateConsumerRequest) (*CreateConsumerResponse, error)
	CreateConsumerGroup(context.Context, *CreateConsumerGroupRequest) (*CreateConsumerGroupResponse, error)
	GetConsumerGroups(context.Context, *GetConsumerGroupsRequest) (*GetConsumerGroupsResponse, error)
	Consume(context.Context, *ConsumeRequest) (*ConsumeResponse, error)
	CreateTopic(context.Context, *CreateTopicRequest) (*CreateTopicResponse, error)
	AckConsume(context.Context, *AckConsumeRequest) (*AckConsumeResponse, error)
	GetMeta(context.Context, *GetMetaRequest) (*GetMetaResponse, error)
	mustEmbedUnimplementedMessageServiceServer()
}

// UnimplementedMessageServiceServer must be embedded to have forward compatible implementations.
type UnimplementedMessageServiceServer struct {
}

func (UnimplementedMessageServiceServer) PublishMessages(context.Context, *PublishMessageRequest) (*PublishMessageResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PublishMessages not implemented")
}
func (UnimplementedMessageServiceServer) CreateConsumer(context.Context, *CreateConsumerRequest) (*CreateConsumerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateConsumer not implemented")
}
func (UnimplementedMessageServiceServer) CreateConsumerGroup(context.Context, *CreateConsumerGroupRequest) (*CreateConsumerGroupResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateConsumerGroup not implemented")
}
func (UnimplementedMessageServiceServer) GetConsumerGroups(context.Context, *GetConsumerGroupsRequest) (*GetConsumerGroupsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetConsumerGroups not implemented")
}
func (UnimplementedMessageServiceServer) Consume(context.Context, *ConsumeRequest) (*ConsumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Consume not implemented")
}
func (UnimplementedMessageServiceServer) CreateTopic(context.Context, *CreateTopicRequest) (*CreateTopicResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateTopic not implemented")
}
func (UnimplementedMessageServiceServer) AckConsume(context.Context, *AckConsumeRequest) (*AckConsumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AckConsume not implemented")
}
func (UnimplementedMessageServiceServer) GetMeta(context.Context, *GetMetaRequest) (*GetMetaResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetMeta not implemented")
}
func (UnimplementedMessageServiceServer) mustEmbedUnimplementedMessageServiceServer() {}

// UnsafeMessageServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MessageServiceServer will
// result in compilation errors.
type UnsafeMessageServiceServer interface {
	mustEmbedUnimplementedMessageServiceServer()
}

func RegisterMessageServiceServer(s grpc.ServiceRegistrar, srv MessageServiceServer) {
	s.RegisterService(&MessageService_ServiceDesc, srv)
}

func _MessageService_PublishMessages_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PublishMessageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MessageServiceServer).PublishMessages(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MessageService_PublishMessages_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MessageServiceServer).PublishMessages(ctx, req.(*PublishMessageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MessageService_CreateConsumer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateConsumerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MessageServiceServer).CreateConsumer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MessageService_CreateConsumer_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MessageServiceServer).CreateConsumer(ctx, req.(*CreateConsumerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MessageService_CreateConsumerGroup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateConsumerGroupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MessageServiceServer).CreateConsumerGroup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MessageService_CreateConsumerGroup_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MessageServiceServer).CreateConsumerGroup(ctx, req.(*CreateConsumerGroupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MessageService_GetConsumerGroups_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetConsumerGroupsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MessageServiceServer).GetConsumerGroups(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MessageService_GetConsumerGroups_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MessageServiceServer).GetConsumerGroups(ctx, req.(*GetConsumerGroupsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MessageService_Consume_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ConsumeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MessageServiceServer).Consume(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MessageService_Consume_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MessageServiceServer).Consume(ctx, req.(*ConsumeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MessageService_CreateTopic_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateTopicRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MessageServiceServer).CreateTopic(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MessageService_CreateTopic_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MessageServiceServer).CreateTopic(ctx, req.(*CreateTopicRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MessageService_AckConsume_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AckConsumeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MessageServiceServer).AckConsume(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MessageService_AckConsume_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MessageServiceServer).AckConsume(ctx, req.(*AckConsumeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MessageService_GetMeta_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetMetaRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MessageServiceServer).GetMeta(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MessageService_GetMeta_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MessageServiceServer).GetMeta(ctx, req.(*GetMetaRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// MessageService_ServiceDesc is the grpc.ServiceDesc for MessageService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var MessageService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "message.MessageService",
	HandlerType: (*MessageServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "PublishMessages",
			Handler:    _MessageService_PublishMessages_Handler,
		},
		{
			MethodName: "CreateConsumer",
			Handler:    _MessageService_CreateConsumer_Handler,
		},
		{
			MethodName: "CreateConsumerGroup",
			Handler:    _MessageService_CreateConsumerGroup_Handler,
		},
		{
			MethodName: "GetConsumerGroups",
			Handler:    _MessageService_GetConsumerGroups_Handler,
		},
		{
			MethodName: "Consume",
			Handler:    _MessageService_Consume_Handler,
		},
		{
			MethodName: "CreateTopic",
			Handler:    _MessageService_CreateTopic_Handler,
		},
		{
			MethodName: "AckConsume",
			Handler:    _MessageService_AckConsume_Handler,
		},
		{
			MethodName: "GetMeta",
			Handler:    _MessageService_GetMeta_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "service.proto",
}
