package internal

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"os"
	"strconv"
	"sync"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

func Get(src, des string) error {
	if src[0] != '/' || src[len(src)-1] == '/' {
		return fmt.Errorf("Get the wrong path: %s\n", src)
	}
	if _, err := os.Stat(des); err == nil {
		return fmt.Errorf("File exists.\n")
	}
	file, err := os.OpenFile(des, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0755)
	defer file.Close()
	if err != nil {
		return err
	}
	checkAndGetArgs := &pb.CheckAndGetArgs{Path: src}
	checkAndGetReply, err := GlobalClientHandler.CheckAndGet(checkAndGetArgs)
	if err != nil {
		logrus.Errorf("fail to check args for get operation. Error detail: %s", err.Error())
		return err
	}
	var (
		chunkNum   = checkAndGetReply.ChunkNum
		fileNodeId = checkAndGetReply.FileNodeId
	)
	GlobalUuid = checkAndGetReply.GetOperationId()
	logrus.Infof("file node id is : %v", fileNodeId)
	logrus.Infof("chunk num is : %v", chunkNum)
	var (
		wg             *sync.WaitGroup
		indexChan      = make(chan int)
		errChan        = make(chan error)
		goroutineCount int
	)
	goroutineCount = maxGoroutineCount
	if maxGoroutineCount > chunkNum {
		goroutineCount = int(chunkNum)
	}
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go produce(fileNodeId, indexChan, errChan, wg, file)
	}
	for i := 0; i < int(chunkNum); i++ {
		indexChan <- i
	}
	return nil
}

func produce(fileNodeId string, index chan int, errChan chan error, wg *sync.WaitGroup, file *os.File) {
	defer wg.Done()
	for chunkIndex := range index {
		getDataNodes4GetArgs := &pb.GetDataNodes4GetArgs{
			FileNodeId: fileNodeId,
			ChunkIndex: int32(chunkIndex),
		}
		getDataNodes4GetReply, err := GlobalClientHandler.GetDataNodes4Get(getDataNodes4GetArgs)
		if err != nil {
			errChan <- err
			file.Close()
			//TODO break的正确性
			break
		}
		var (
			dataNodeIds      = getDataNodes4GetReply.DataNodeIds
			dataNodeAddrs    = getDataNodes4GetReply.DataNodeAddrs
			primaryNodeIndex = 0
			chunkId          = fileNodeId + common.ChunkIdDelimiter + strconv.Itoa(chunkIndex)
		)
		setupStream2DataNodeArgs := &pb.SetupStream2DataNodeArgs{
			ClientPort: viper.GetString(common.ChunkPort),
			ChunkId:    chunkId,
			DataNodeId: dataNodeIds[primaryNodeIndex],
		}
		_, err = GlobalClientHandler.SetupStream2DataNode(
			dataNodeAddrs[primaryNodeIndex], setupStream2DataNodeArgs)
		// if primary datanode fails, client will try to connect the next datanode
		for err != nil {
			primaryNodeIndex++
			if primaryNodeIndex >= len(dataNodeAddrs) {
				errChan <- fmt.Errorf("All of dataNode's file is ruined.FileNodeId = %s\n", fileNodeId)
				file.Close()
				//TODO return的正确性
				return
			}
			_, err = GlobalClientHandler.SetupStream2DataNode(
				dataNodeAddrs[primaryNodeIndex], setupStream2DataNodeArgs)
		}
	}
}

// getReadStream Build stream to get data from primary chunkserver to this chunk.
func getReadStream(chunkId, primaryNodeAddr string) (pb.PipLineService_TransferChunkClient, error) {
	conn, _ := grpc.Dial(primaryNodeAddr+common.AddressDelimiter+viper.GetString(common.ChunkPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	c := pb.NewPipLineServiceClient(conn)
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, common.ChunkIdString, chunkId)
	return c.TransferChunk(ctx)
}
