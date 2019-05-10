// Code generated by protoc-gen-go. DO NOT EDIT.
// source: custom_rls.proto

package envoy_service_ratelimit_v2

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import ratelimit "github.com/lyft/ratelimit/proto/envoy/api/v2/ratelimit"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// RateLimitDescriptor from public import envoy/api/v2/ratelimit/ratelimit.proto
type RateLimitDescriptor = ratelimit.RateLimitDescriptor

// Entry from public import envoy/api/v2/ratelimit/ratelimit.proto
type RateLimitDescriptor_Entry = ratelimit.RateLimitDescriptor_Entry

type RateLimitResponse_Code int32

const (
	RateLimitResponse_UNKNOWN    RateLimitResponse_Code = 0
	RateLimitResponse_OK         RateLimitResponse_Code = 1
	RateLimitResponse_OVER_LIMIT RateLimitResponse_Code = 2
)

var RateLimitResponse_Code_name = map[int32]string{
	0: "UNKNOWN",
	1: "OK",
	2: "OVER_LIMIT",
}
var RateLimitResponse_Code_value = map[string]int32{
	"UNKNOWN":    0,
	"OK":         1,
	"OVER_LIMIT": 2,
}

func (x RateLimitResponse_Code) String() string {
	return proto.EnumName(RateLimitResponse_Code_name, int32(x))
}
func (RateLimitResponse_Code) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_custom_rls_39c81ce902bc96b9, []int{1, 0}
}

type RateLimitResponse_RateLimit_Unit int32

const (
	RateLimitResponse_RateLimit_UNKNOWN RateLimitResponse_RateLimit_Unit = 0
	RateLimitResponse_RateLimit_SECOND  RateLimitResponse_RateLimit_Unit = 1
	RateLimitResponse_RateLimit_MINUTE  RateLimitResponse_RateLimit_Unit = 2
	RateLimitResponse_RateLimit_HOUR    RateLimitResponse_RateLimit_Unit = 3
	RateLimitResponse_RateLimit_DAY     RateLimitResponse_RateLimit_Unit = 4
)

var RateLimitResponse_RateLimit_Unit_name = map[int32]string{
	0: "UNKNOWN",
	1: "SECOND",
	2: "MINUTE",
	3: "HOUR",
	4: "DAY",
}
var RateLimitResponse_RateLimit_Unit_value = map[string]int32{
	"UNKNOWN": 0,
	"SECOND":  1,
	"MINUTE":  2,
	"HOUR":    3,
	"DAY":     4,
}

func (x RateLimitResponse_RateLimit_Unit) String() string {
	return proto.EnumName(RateLimitResponse_RateLimit_Unit_name, int32(x))
}
func (RateLimitResponse_RateLimit_Unit) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_custom_rls_39c81ce902bc96b9, []int{1, 0, 0}
}

// Main message for a rate limit request. The rate limit service is designed to be fully generic
// in the sense that it can operate on arbitrary hierarchical key/value pairs. The loaded
// configuration will parse the request and find the most specific limit to apply. In addition,
// a RateLimitRequest can contain multiple "descriptors" to limit on. When multiple descriptors
// are provided, the server will limit on *ALL* of them and return an OVER_LIMIT response if any
// of them are over limit. This enables more complex application level rate limiting scenarios
// if desired.
// [#not-implemented-hide:] Hiding API for now.
type RateLimitRequest struct {
	// All rate limit requests must specify a domain. This enables the configuration to be per
	// application without fear of overlap. E.g., "envoy".
	Domain string `protobuf:"bytes,1,opt,name=domain" json:"domain,omitempty"`
	// All rate limit requests must specify at least one RateLimitDescriptor. Each descriptor is
	// processed by the service (see below). If any of the descriptors are over limit, the entire
	// request is considered to be over limit.
	Descriptors []*ratelimit.RateLimitDescriptor `protobuf:"bytes,2,rep,name=descriptors" json:"descriptors,omitempty"`
	// Rate limit requests can optionally specify the number of hits a request adds to the matched
	// limit. If the value is not set in the message, a request increases the matched limit by 1.
	HitsAddend           uint32   `protobuf:"varint,3,opt,name=hits_addend,json=hitsAddend" json:"hits_addend,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RateLimitRequest) Reset()         { *m = RateLimitRequest{} }
func (m *RateLimitRequest) String() string { return proto.CompactTextString(m) }
func (*RateLimitRequest) ProtoMessage()    {}
func (*RateLimitRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_custom_rls_39c81ce902bc96b9, []int{0}
}
func (m *RateLimitRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RateLimitRequest.Unmarshal(m, b)
}
func (m *RateLimitRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RateLimitRequest.Marshal(b, m, deterministic)
}
func (dst *RateLimitRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RateLimitRequest.Merge(dst, src)
}
func (m *RateLimitRequest) XXX_Size() int {
	return xxx_messageInfo_RateLimitRequest.Size(m)
}
func (m *RateLimitRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_RateLimitRequest.DiscardUnknown(m)
}

var xxx_messageInfo_RateLimitRequest proto.InternalMessageInfo

func (m *RateLimitRequest) GetDomain() string {
	if m != nil {
		return m.Domain
	}
	return ""
}

func (m *RateLimitRequest) GetDescriptors() []*ratelimit.RateLimitDescriptor {
	if m != nil {
		return m.Descriptors
	}
	return nil
}

func (m *RateLimitRequest) GetHitsAddend() uint32 {
	if m != nil {
		return m.HitsAddend
	}
	return 0
}

// A response from a ShouldRateLimit call.
// [#not-implemented-hide:] Hiding API for now.
type RateLimitResponse struct {
	// The overall response code which takes into account all of the descriptors that were passed
	// in the RateLimitRequest message.
	OverallCode RateLimitResponse_Code `protobuf:"varint,1,opt,name=overall_code,json=overallCode,enum=envoy.service.ratelimit.v2.RateLimitResponse_Code" json:"overall_code,omitempty"`
	// A list of DescriptorStatus messages which matches the length of the descriptor list passed
	// in the RateLimitRequest. This can be used by the caller to determine which individual
	// descriptors failed and/or what the currently configured limits are for all of them.
	Statuses             []*RateLimitResponse_DescriptorStatus `protobuf:"bytes,2,rep,name=statuses" json:"statuses,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                              `json:"-"`
	XXX_unrecognized     []byte                                `json:"-"`
	XXX_sizecache        int32                                 `json:"-"`
}

func (m *RateLimitResponse) Reset()         { *m = RateLimitResponse{} }
func (m *RateLimitResponse) String() string { return proto.CompactTextString(m) }
func (*RateLimitResponse) ProtoMessage()    {}
func (*RateLimitResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_custom_rls_39c81ce902bc96b9, []int{1}
}
func (m *RateLimitResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RateLimitResponse.Unmarshal(m, b)
}
func (m *RateLimitResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RateLimitResponse.Marshal(b, m, deterministic)
}
func (dst *RateLimitResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RateLimitResponse.Merge(dst, src)
}
func (m *RateLimitResponse) XXX_Size() int {
	return xxx_messageInfo_RateLimitResponse.Size(m)
}
func (m *RateLimitResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_RateLimitResponse.DiscardUnknown(m)
}

var xxx_messageInfo_RateLimitResponse proto.InternalMessageInfo

func (m *RateLimitResponse) GetOverallCode() RateLimitResponse_Code {
	if m != nil {
		return m.OverallCode
	}
	return RateLimitResponse_UNKNOWN
}

func (m *RateLimitResponse) GetStatuses() []*RateLimitResponse_DescriptorStatus {
	if m != nil {
		return m.Statuses
	}
	return nil
}

// Defines an actual rate limit in terms of requests per unit of time and the unit itself.
type RateLimitResponse_RateLimit struct {
	RequestsPerUnit      uint32                           `protobuf:"varint,1,opt,name=requests_per_unit,json=requestsPerUnit" json:"requests_per_unit,omitempty"`
	Duration             uint32                           `protobuf:"varint,2,opt,name=duration" json:"duration,omitempty"`
	Unit                 RateLimitResponse_RateLimit_Unit `protobuf:"varint,3,opt,name=unit,enum=envoy.service.ratelimit.v2.RateLimitResponse_RateLimit_Unit" json:"unit,omitempty"`
	TokenBucketCapacity  float64                          `protobuf:"fixed64,4,opt,name=token_bucket_capacity,json=tokenBucketCapacity" json:"token_bucket_capacity,omitempty"`
	AlgType              string                           `protobuf:"bytes,5,opt,name=alg_type,json=algType" json:"alg_type,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                         `json:"-"`
	XXX_unrecognized     []byte                           `json:"-"`
	XXX_sizecache        int32                            `json:"-"`
}

func (m *RateLimitResponse_RateLimit) Reset()         { *m = RateLimitResponse_RateLimit{} }
func (m *RateLimitResponse_RateLimit) String() string { return proto.CompactTextString(m) }
func (*RateLimitResponse_RateLimit) ProtoMessage()    {}
func (*RateLimitResponse_RateLimit) Descriptor() ([]byte, []int) {
	return fileDescriptor_custom_rls_39c81ce902bc96b9, []int{1, 0}
}
func (m *RateLimitResponse_RateLimit) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RateLimitResponse_RateLimit.Unmarshal(m, b)
}
func (m *RateLimitResponse_RateLimit) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RateLimitResponse_RateLimit.Marshal(b, m, deterministic)
}
func (dst *RateLimitResponse_RateLimit) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RateLimitResponse_RateLimit.Merge(dst, src)
}
func (m *RateLimitResponse_RateLimit) XXX_Size() int {
	return xxx_messageInfo_RateLimitResponse_RateLimit.Size(m)
}
func (m *RateLimitResponse_RateLimit) XXX_DiscardUnknown() {
	xxx_messageInfo_RateLimitResponse_RateLimit.DiscardUnknown(m)
}

var xxx_messageInfo_RateLimitResponse_RateLimit proto.InternalMessageInfo

func (m *RateLimitResponse_RateLimit) GetRequestsPerUnit() uint32 {
	if m != nil {
		return m.RequestsPerUnit
	}
	return 0
}

func (m *RateLimitResponse_RateLimit) GetDuration() uint32 {
	if m != nil {
		return m.Duration
	}
	return 0
}

func (m *RateLimitResponse_RateLimit) GetUnit() RateLimitResponse_RateLimit_Unit {
	if m != nil {
		return m.Unit
	}
	return RateLimitResponse_RateLimit_UNKNOWN
}

func (m *RateLimitResponse_RateLimit) GetTokenBucketCapacity() float64 {
	if m != nil {
		return m.TokenBucketCapacity
	}
	return 0
}

func (m *RateLimitResponse_RateLimit) GetAlgType() string {
	if m != nil {
		return m.AlgType
	}
	return ""
}

type RateLimitResponse_DescriptorStatus struct {
	// The response code for an individual descriptor.
	Code RateLimitResponse_Code `protobuf:"varint,1,opt,name=code,enum=envoy.service.ratelimit.v2.RateLimitResponse_Code" json:"code,omitempty"`
	// The current limit as configured by the server. Useful for debugging, etc.
	CurrentLimit *RateLimitResponse_RateLimit `protobuf:"bytes,2,opt,name=current_limit,json=currentLimit" json:"current_limit,omitempty"`
	// The limit remaining in the current time unit.
	LimitRemaining       uint32   `protobuf:"varint,3,opt,name=limit_remaining,json=limitRemaining" json:"limit_remaining,omitempty"`
	TokensRemaining      float64  `protobuf:"fixed64,4,opt,name=tokens_remaining,json=tokensRemaining" json:"tokens_remaining,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RateLimitResponse_DescriptorStatus) Reset()         { *m = RateLimitResponse_DescriptorStatus{} }
func (m *RateLimitResponse_DescriptorStatus) String() string { return proto.CompactTextString(m) }
func (*RateLimitResponse_DescriptorStatus) ProtoMessage()    {}
func (*RateLimitResponse_DescriptorStatus) Descriptor() ([]byte, []int) {
	return fileDescriptor_custom_rls_39c81ce902bc96b9, []int{1, 1}
}
func (m *RateLimitResponse_DescriptorStatus) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RateLimitResponse_DescriptorStatus.Unmarshal(m, b)
}
func (m *RateLimitResponse_DescriptorStatus) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RateLimitResponse_DescriptorStatus.Marshal(b, m, deterministic)
}
func (dst *RateLimitResponse_DescriptorStatus) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RateLimitResponse_DescriptorStatus.Merge(dst, src)
}
func (m *RateLimitResponse_DescriptorStatus) XXX_Size() int {
	return xxx_messageInfo_RateLimitResponse_DescriptorStatus.Size(m)
}
func (m *RateLimitResponse_DescriptorStatus) XXX_DiscardUnknown() {
	xxx_messageInfo_RateLimitResponse_DescriptorStatus.DiscardUnknown(m)
}

var xxx_messageInfo_RateLimitResponse_DescriptorStatus proto.InternalMessageInfo

func (m *RateLimitResponse_DescriptorStatus) GetCode() RateLimitResponse_Code {
	if m != nil {
		return m.Code
	}
	return RateLimitResponse_UNKNOWN
}

func (m *RateLimitResponse_DescriptorStatus) GetCurrentLimit() *RateLimitResponse_RateLimit {
	if m != nil {
		return m.CurrentLimit
	}
	return nil
}

func (m *RateLimitResponse_DescriptorStatus) GetLimitRemaining() uint32 {
	if m != nil {
		return m.LimitRemaining
	}
	return 0
}

func (m *RateLimitResponse_DescriptorStatus) GetTokensRemaining() float64 {
	if m != nil {
		return m.TokensRemaining
	}
	return 0
}

func init() {
	proto.RegisterType((*RateLimitRequest)(nil), "envoy.service.ratelimit.v2.RateLimitRequest")
	proto.RegisterType((*RateLimitResponse)(nil), "envoy.service.ratelimit.v2.RateLimitResponse")
	proto.RegisterType((*RateLimitResponse_RateLimit)(nil), "envoy.service.ratelimit.v2.RateLimitResponse.RateLimit")
	proto.RegisterType((*RateLimitResponse_DescriptorStatus)(nil), "envoy.service.ratelimit.v2.RateLimitResponse.DescriptorStatus")
	proto.RegisterEnum("envoy.service.ratelimit.v2.RateLimitResponse_Code", RateLimitResponse_Code_name, RateLimitResponse_Code_value)
	proto.RegisterEnum("envoy.service.ratelimit.v2.RateLimitResponse_RateLimit_Unit", RateLimitResponse_RateLimit_Unit_name, RateLimitResponse_RateLimit_Unit_value)
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for RateLimitService service

type RateLimitServiceClient interface {
	// Determine whether rate limiting should take place.
	ShouldRateLimit(ctx context.Context, in *RateLimitRequest, opts ...grpc.CallOption) (*RateLimitResponse, error)
}

type rateLimitServiceClient struct {
	cc *grpc.ClientConn
}

func NewRateLimitServiceClient(cc *grpc.ClientConn) RateLimitServiceClient {
	return &rateLimitServiceClient{cc}
}

func (c *rateLimitServiceClient) ShouldRateLimit(ctx context.Context, in *RateLimitRequest, opts ...grpc.CallOption) (*RateLimitResponse, error) {
	out := new(RateLimitResponse)
	err := grpc.Invoke(ctx, "/envoy.service.ratelimit.v2.RateLimitService/ShouldRateLimit", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for RateLimitService service

type RateLimitServiceServer interface {
	// Determine whether rate limiting should take place.
	ShouldRateLimit(context.Context, *RateLimitRequest) (*RateLimitResponse, error)
}

func RegisterRateLimitServiceServer(s *grpc.Server, srv RateLimitServiceServer) {
	s.RegisterService(&_RateLimitService_serviceDesc, srv)
}

func _RateLimitService_ShouldRateLimit_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RateLimitRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RateLimitServiceServer).ShouldRateLimit(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/envoy.service.ratelimit.v2.RateLimitService/ShouldRateLimit",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RateLimitServiceServer).ShouldRateLimit(ctx, req.(*RateLimitRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _RateLimitService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "envoy.service.ratelimit.v2.RateLimitService",
	HandlerType: (*RateLimitServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ShouldRateLimit",
			Handler:    _RateLimitService_ShouldRateLimit_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "custom_rls.proto",
}

func init() { proto.RegisterFile("custom_rls.proto", fileDescriptor_custom_rls_39c81ce902bc96b9) }

var fileDescriptor_custom_rls_39c81ce902bc96b9 = []byte{
	// 582 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xa4, 0x54, 0x51, 0x6f, 0xd3, 0x3c,
	0x14, 0x5d, 0xd2, 0x7c, 0x5d, 0x77, 0xb3, 0xad, 0x99, 0x3f, 0x81, 0x42, 0x5e, 0xa8, 0xfa, 0x00,
	0x85, 0x41, 0x26, 0x85, 0x07, 0x5e, 0xd0, 0xa4, 0xb1, 0x0d, 0x31, 0x6d, 0x6b, 0x27, 0x77, 0x05,
	0x81, 0x90, 0x22, 0x2f, 0xb1, 0x3a, 0x6b, 0x69, 0x1c, 0x6c, 0x27, 0x52, 0xdf, 0x79, 0xe4, 0x17,
	0xf0, 0x3b, 0x78, 0xe1, 0xdf, 0xa1, 0x38, 0x59, 0x5a, 0x40, 0x48, 0x14, 0xde, 0xec, 0x7b, 0x7c,
	0x4e, 0xee, 0x39, 0xbe, 0x0e, 0x38, 0x51, 0x2e, 0x15, 0x9f, 0x85, 0x22, 0x91, 0x7e, 0x26, 0xb8,
	0xe2, 0xc8, 0xa3, 0x69, 0xc1, 0xe7, 0xbe, 0xa4, 0xa2, 0x60, 0x11, 0xf5, 0x05, 0x51, 0x34, 0x61,
	0x33, 0xa6, 0xfc, 0x22, 0xf0, 0x1e, 0x68, 0x6c, 0x8f, 0x64, 0x6c, 0xaf, 0x08, 0xf6, 0x1a, 0x68,
	0xb1, 0xaa, 0x34, 0xfa, 0x5f, 0x0c, 0x70, 0x30, 0x51, 0xf4, 0xac, 0xac, 0x61, 0xfa, 0x31, 0xa7,
	0x52, 0xa1, 0xbb, 0xd0, 0x8e, 0xf9, 0x8c, 0xb0, 0xd4, 0x35, 0x7a, 0xc6, 0x60, 0x03, 0xd7, 0x3b,
	0x74, 0x0e, 0x76, 0x4c, 0x65, 0x24, 0x58, 0xa6, 0xb8, 0x90, 0xae, 0xd9, 0x6b, 0x0d, 0xec, 0x60,
	0xd7, 0xaf, 0xda, 0x20, 0x19, 0xf3, 0x8b, 0x60, 0xa9, 0x8b, 0x46, 0xf6, 0xa8, 0xe1, 0xe0, 0x65,
	0x3e, 0xba, 0x0f, 0xf6, 0x35, 0x53, 0x32, 0x24, 0x71, 0x4c, 0xd3, 0xd8, 0x6d, 0xf5, 0x8c, 0xc1,
	0x16, 0x86, 0xb2, 0x74, 0xa0, 0x2b, 0xfd, 0x6f, 0x6d, 0xd8, 0x59, 0x6a, 0x4e, 0x66, 0x3c, 0x95,
	0x14, 0x4d, 0x60, 0x93, 0x17, 0x54, 0x90, 0x24, 0x09, 0x23, 0x1e, 0x53, 0xdd, 0xe3, 0x76, 0x10,
	0xf8, 0xbf, 0x4f, 0xc3, 0xff, 0x45, 0xc4, 0x3f, 0xe4, 0x31, 0xc5, 0x76, 0xad, 0x53, 0x6e, 0xd0,
	0x7b, 0xe8, 0x48, 0x45, 0x54, 0x2e, 0xe9, 0xad, 0xb3, 0xfd, 0xd5, 0x24, 0x17, 0x36, 0xc7, 0x5a,
	0x07, 0x37, 0x7a, 0xde, 0x57, 0x13, 0x36, 0x1a, 0x02, 0x7a, 0x0c, 0x3b, 0xa2, 0x4a, 0x5a, 0x86,
	0x19, 0x15, 0x61, 0x9e, 0x32, 0xa5, 0x5d, 0x6c, 0xe1, 0xee, 0x2d, 0x70, 0x41, 0xc5, 0x24, 0x65,
	0x0a, 0x79, 0xd0, 0x89, 0x73, 0x41, 0x14, 0xe3, 0xa9, 0x6b, 0xea, 0x23, 0xcd, 0x1e, 0x5d, 0x80,
	0xa5, 0xa9, 0x2d, 0x1d, 0xc0, 0x8b, 0xd5, 0xba, 0x6d, 0x2a, 0x7e, 0xf9, 0x1d, 0xac, 0x95, 0x50,
	0x00, 0x77, 0x14, 0xbf, 0xa1, 0x69, 0x78, 0x95, 0x47, 0x37, 0x54, 0x85, 0x11, 0xc9, 0x48, 0xc4,
	0xd4, 0xdc, 0xb5, 0x7a, 0xc6, 0xc0, 0xc0, 0xff, 0x6b, 0xf0, 0xa5, 0xc6, 0x0e, 0x6b, 0x08, 0xdd,
	0x83, 0x0e, 0x49, 0xa6, 0xa1, 0x9a, 0x67, 0xd4, 0xfd, 0x4f, 0x8f, 0xcb, 0x3a, 0x49, 0xa6, 0x97,
	0xf3, 0x8c, 0xf6, 0xf7, 0xc1, 0xd2, 0x26, 0x6c, 0x58, 0x9f, 0x0c, 0x4f, 0x87, 0xa3, 0xb7, 0x43,
	0x67, 0x0d, 0x01, 0xb4, 0xc7, 0xc7, 0x87, 0xa3, 0xe1, 0x91, 0x63, 0x94, 0xeb, 0xf3, 0x93, 0xe1,
	0xe4, 0xf2, 0xd8, 0x31, 0x51, 0x07, 0xac, 0xd7, 0xa3, 0x09, 0x76, 0x5a, 0x68, 0x1d, 0x5a, 0x47,
	0x07, 0xef, 0x1c, 0xcb, 0xfb, 0x6c, 0x82, 0xf3, 0x73, 0xaa, 0xe8, 0x15, 0x58, 0xff, 0x78, 0xed,
	0x9a, 0x8f, 0x3e, 0xc0, 0x56, 0x94, 0x0b, 0x41, 0x53, 0x15, 0x6a, 0x82, 0x8e, 0xd7, 0x0e, 0x9e,
	0xff, 0x65, 0x8c, 0x78, 0xb3, 0x56, 0xab, 0xee, 0xf8, 0x21, 0x74, 0x35, 0x2b, 0x14, 0xb4, 0x7c,
	0x3a, 0x2c, 0x9d, 0xd6, 0xf3, 0xbd, 0x9d, 0x54, 0xfc, 0xba, 0x8a, 0x1e, 0x81, 0xa3, 0x53, 0x95,
	0x4b, 0x27, 0xab, 0xb4, 0xbb, 0x55, 0xbd, 0x39, 0xda, 0xdf, 0x05, 0x4b, 0x4f, 0xea, 0x0f, 0x71,
	0xb6, 0xc1, 0x1c, 0x9d, 0x3a, 0x06, 0xda, 0x06, 0x18, 0xbd, 0x39, 0xc6, 0xe1, 0xd9, 0xc9, 0xf9,
	0xc9, 0xa5, 0x63, 0x06, 0x9f, 0x96, 0x1f, 0xf6, 0xb8, 0x32, 0x83, 0x32, 0xe8, 0x8e, 0xaf, 0x79,
	0x9e, 0xc4, 0x8b, 0x61, 0x7c, 0xf2, 0x87, 0x7e, 0xf5, 0x58, 0x7a, 0x4f, 0x57, 0x4a, 0xa7, 0xbf,
	0x76, 0xb1, 0x76, 0xd5, 0xd6, 0x3f, 0x9a, 0x67, 0xdf, 0x03, 0x00, 0x00, 0xff, 0xff, 0x9a, 0x3e,
	0x25, 0x01, 0xc0, 0x04, 0x00, 0x00,
}
