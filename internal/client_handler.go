package internal

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"os"
	"strconv"
	"sync"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

var (
	GlobalClientHandler *ClientHandler
	once                = &sync.Once{}
	file                *os.File //Available in Get operation
)

const (
	ChunkIndex = "ChunkIndex"
)

type ClientHandler struct {
	pb.UnimplementedMasterAddServiceServer
	pb.UnimplementedMasterMkdirServiceServer
	pb.UnimplementedMasterMoveServiceServer
	pb.UnimplementedMasterRemoveServiceServer
	pb.UnimplementedMasterStatServiceServer
	pb.UnimplementedMasterListServiceServer
	pb.UnimplementedMasterRenameServiceServer
	pb.UnimplementedMasterGetServiceServer
	pb.UnimplementedPipLineServiceServer
}

func init() {
	if GlobalClientHandler == nil {
		once.Do(func() {
			GlobalClientHandler = &ClientHandler{}
		})
	}
}

func (c *ClientHandler) Check4Add(args *pb.CheckArgs4AddArgs) (*pb.CheckArgs4AddReply, error) {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewMasterAddServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckArgs4Add(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndGet(args *pb.CheckAndGetArgs) (*pb.CheckAndGetReply, error) {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewMasterGetServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndGet(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndStat(args *pb.CheckAndStatArgs) (*pb.CheckAndStatReply, error) {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewMasterStatServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndStat(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndList(args *pb.CheckAndListArgs) (*pb.CheckAndListReply, error) {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewMasterListServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndList(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndRename(args *pb.CheckAndRenameArgs) (*pb.CheckAndRenameReply, error) {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewMasterRenameServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndRename(ctx, args)
	return reply, err
}

func (c *ClientHandler) GetDataNodes4Add(args *pb.GetDataNodes4AddArgs) (*pb.GetDataNodes4AddReply, error) {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewMasterAddServiceClient(conn)
	ctx := context.Background()
	reply, err := client.GetDataNodes4Add(ctx, args)
	return reply, err
}

func (c *ClientHandler) GetDataNodes4Get(args *pb.GetDataNodes4GetArgs) (*pb.GetDataNodes4GetReply, error) {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewMasterGetServiceClient(conn)
	ctx := context.Background()
	reply, err := client.GetDataNodes4Get(ctx, args)
	return reply, err
}

func (c *ClientHandler) SetupStream2DataNode(addr string, args *pb.SetupStream2DataNodeArgs) (*pb.SetupStream2DataNodeReply, error) {
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewMasterGetServiceClient(conn)
	ctx := context.Background()
	reply, err := client.SetupStream2DataNode(ctx, args)
	return reply, err
}

//TransferChunk called by chunkserver
//TransferChunk receive the file data with specified index from cs
func (c *ClientHandler) TransferChunk(stream pb.PipLineService_TransferChunkServer) error {
	p, _ := peer.FromContext(stream.Context())
	md, _ := metadata.FromIncomingContext(stream.Context())
	index := md.Get(ChunkIndex)[0]
	address := p.Addr.String()
	logrus.Infof("start to receive chunk from: %s. Index of %s\n", address, index)
	parseInt, _ := strconv.ParseInt(index, 10, 32)
	err := DoTransferFile(stream, int32(parseInt))
	if err != nil {
		return err
	}
	return nil
}

func (c *ClientHandler) UnlockDic4Add(args *pb.UnlockDic4AddArgs) (*pb.UnlockDic4AddReply, error) {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewMasterAddServiceClient(conn)
	ctx := context.Background()
	reply, err := client.UnlockDic4Add(ctx, args)
	return reply, err
}

func (c *ClientHandler) ReleaseLease4Add(args *pb.ReleaseLease4AddArgs) (*pb.ReleaseLease4AddReply, error) {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewMasterAddServiceClient(conn)
	ctx := context.Background()
	reply, err := client.ReleaseLease4Add(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndMkdir(args *pb.CheckAndMkDirArgs) (*pb.CheckAndMkDirReply, error) {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewMasterMkdirServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndMkdir(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndMove(args *pb.CheckAndMoveArgs) (*pb.CheckAndMoveReply, error) {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewMasterMoveServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndMove(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndRemove(args *pb.CheckAndRemoveArgs) (*pb.CheckAndRemoveReply, error) {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewMasterRemoveServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndRemove(ctx, args)
	return reply, err
}
