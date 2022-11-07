package internal

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/config"
	"tinydfs-base/protocol/pb"
)

var (
	GlobalClientHandler *ClientHandler
	once                = &sync.Once{}
)

type ClientHandler struct {
	EtcdClient *clientv3.Client
	pb.UnimplementedMasterAddServiceServer
	pb.UnimplementedMasterMkdirServiceServer
	pb.UnimplementedMasterMoveServiceServer
	pb.UnimplementedMasterRemoveServiceServer
	pb.UnimplementedMasterStatServiceServer
	pb.UnimplementedMasterListServiceServer
	pb.UnimplementedMasterRenameServiceServer
	pb.UnimplementedMasterGetServiceServer
	pb.UnimplementedPipLineServiceServer
	pb.UnimplementedSetupStreamServer
}

var Logger *logrus.Logger

func init() {
	config.InitConfig()
	Logger = config.InitLogger(Logger, false)
	Logger.SetLevel(logrus.Level(viper.GetInt(common.ClientLogLevel)))
	if GlobalClientHandler == nil {
		once.Do(func() {
			var err error
			GlobalClientHandler = &ClientHandler{}
			Logger.Infof("etcd address: %s", viper.GetString(common.EtcdEndPoint))
			GlobalClientHandler.EtcdClient, err = clientv3.New(clientv3.Config{
				Endpoints:   []string{viper.GetString(common.EtcdEndPoint)},
				DialTimeout: 5 * time.Second,
			})
			if err != nil {
				logrus.Panicf("Fail to get etcd client, error detail : %s", err.Error())
			}
		})
	}
}

func (c *ClientHandler) Check4Add(args *pb.CheckArgs4AddArgs) (*pb.CheckArgs4AddReply, error) {
	conn, err := getLeaderConn()
	if err != nil {
		return nil, err
	}
	client := pb.NewMasterAddServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckArgs4Add(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndGet(args *pb.CheckAndGetArgs) (*pb.CheckAndGetReply, error) {
	conn, err := getLeaderConn()
	if err != nil {
		return nil, err
	}
	client := pb.NewMasterGetServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndGet(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndStat(args *pb.CheckAndStatArgs) (*pb.CheckAndStatReply, error) {
	var (
		conn *grpc.ClientConn
		err  error
	)
	if args.IsLatest {
		conn, err = getLeaderConn()
		if err != nil {
			return nil, err
		}
	} else {
		conn, err = getFollowerConn()
		if err != nil {
			return nil, err
		}
	}
	client := pb.NewMasterStatServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndStat(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndList(args *pb.CheckAndListArgs) (*pb.CheckAndListReply, error) {
	var (
		conn *grpc.ClientConn
		err  error
	)
	if args.IsLatest {
		conn, err = getLeaderConn()
		if err != nil {
			return nil, err
		}
	} else {
		conn, err = getFollowerConn()
		if err != nil {
			return nil, err
		}
	}
	client := pb.NewMasterListServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndList(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndRename(args *pb.CheckAndRenameArgs) (*pb.CheckAndRenameReply, error) {
	conn, err := getLeaderConn()
	if err != nil {
		return nil, err
	}
	client := pb.NewMasterRenameServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndRename(ctx, args)
	return reply, err
}

func (c *ClientHandler) GetDataNodes4Add(args *pb.GetDataNodes4AddArgs) (*pb.GetDataNodes4AddReply, error) {
	conn, err := getLeaderConn()
	if err != nil {
		return nil, err
	}
	client := pb.NewMasterAddServiceClient(conn)
	ctx := context.Background()
	reply, err := client.GetDataNodes4Add(ctx, args)
	return reply, err
}

func (c *ClientHandler) GetDataNodes4Get(args *pb.GetDataNodes4GetArgs) (*pb.GetDataNodes4GetReply, error) {
	conn, err := getLeaderConn()
	if err != nil {
		return nil, err
	}
	client := pb.NewMasterGetServiceClient(conn)
	ctx := context.Background()
	reply, err := client.GetDataNodes4Get(ctx, args)
	return reply, err
}

func (c *ClientHandler) SetupStream2DataNode(addr string, args *pb.SetupStream2DataNodeArgs) (pb.SetupStream_SetupStream2DataNodeClient, error) {
	addr = addr + common.AddressDelimiter + viper.GetString(common.ChunkPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewSetupStreamClient(conn)
	ctx := context.Background()
	reply, err := client.SetupStream2DataNode(ctx, args)
	return reply, err
}

func (c *ClientHandler) Callback4Add(args *pb.Callback4AddArgs) (*pb.Callback4AddReply, error) {
	conn, err := getLeaderConn()
	if err != nil {
		return nil, err
	}
	client := pb.NewMasterAddServiceClient(conn)
	ctx := context.Background()
	reply, err := client.Callback4Add(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndMkdir(args *pb.CheckAndMkDirArgs) (*pb.CheckAndMkDirReply, error) {
	conn, err := getLeaderConn()
	if err != nil {
		return nil, err
	}
	client := pb.NewMasterMkdirServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndMkdir(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndMove(args *pb.CheckAndMoveArgs) (*pb.CheckAndMoveReply, error) {
	conn, err := getLeaderConn()
	if err != nil {
		return nil, err
	}
	client := pb.NewMasterMoveServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndMove(ctx, args)
	return reply, err
}

func (c *ClientHandler) CheckAndRemove(args *pb.CheckAndRemoveArgs) (*pb.CheckAndRemoveReply, error) {
	conn, err := getLeaderConn()
	if err != nil {
		return nil, err
	}
	client := pb.NewMasterRemoveServiceClient(conn)
	ctx := context.Background()
	reply, err := client.CheckAndRemove(ctx, args)
	return reply, err
}

func getLeaderConn() (*grpc.ClientConn, error) {
	ctx := context.Background()
	kv := clientv3.NewKV(GlobalClientHandler.EtcdClient)
	getResp, err := kv.Get(ctx, common.LeaderAddressKey)
	if err != nil {
		logrus.Errorf("Fail to get kv when init, error detail: %s", err.Error())
		return nil, err
	}
	addr := string(getResp.Kvs[0].Value)
	addr = strings.Split(addr, common.AddressDelimiter)[0] + viper.GetString(common.MasterPort)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logrus.Errorf("Fail to get connection to leader , error detail: %s", err.Error())
		return nil, err
	}
	return conn, nil
}

func getFollowerConn() (*grpc.ClientConn, error) {
	ctx := context.Background()
	kv := clientv3.NewKV(GlobalClientHandler.EtcdClient)
	getResp, err := kv.Get(ctx, common.FollowerKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		logrus.Errorf("Fail to get kv when init, error detail: %s", err.Error())
		return nil, err
	}
	rand.Seed(time.Now().UnixNano())
	addr := string(getResp.Kvs[rand.Intn(len(getResp.Kvs))].Value)
	addr = strings.Split(addr, common.AddressDelimiter)[0] + viper.GetString(common.MasterPort)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logrus.Errorf("Fail to get connection to leader , error detail: %s", err.Error())
		return nil, err
	}
	return conn, nil
}

func (c *ClientHandler) Server() {
	listener, err := net.Listen(common.TCP, common.AddressDelimiter+viper.GetString(common.ClientPort))
	if err != nil {
		logrus.Errorf("Fail to server, error code: %v, error detail: %s,", common.ChunkServerRPCServerFailed, err.Error())
		os.Exit(1)
	}
	server := grpc.NewServer()
	pb.RegisterPipLineServiceServer(server, c)
	logrus.Infof("Client is running, listen on :%s", viper.GetString(common.ClientPort))
	server.Serve(listener)
}
