package internal

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
	"tinydfs-base/util"
)

const (
	pathSplitString   = "/"
	maxGoroutineCount = 5
)

func Add(src, des string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}

	desPath := strings.Split(des, pathSplitString)
	if desPath[0] != "" {
		return fmt.Errorf("Get the wrong path: %s\n", des)
	}
	desPathLength := len(desPath)
	var fileName string
	var targetPath string
	if desPath[desPathLength-1] == "" {
		// 表示 des 为指定目录
		srcPath := strings.Split(src, pathSplitString)
		srcPathLength := len(srcPath)
		// 文件名从 src 处获取
		fileName = srcPath[srcPathLength-1]
		targetPath = des
	} else {
		// 表示 des 为指定目录下的文件
		fileName = desPath[desPathLength-1]
		targetPath = strings.Join(desPath[:desPathLength-1], pathSplitString)
	}

	checkArgs4AddArgs := &pb.CheckArgs4AddArgs{
		Path:     targetPath,
		FileName: fileName,
		Size:     info.Size(),
	}
	checkArgs4AddReply, err := GlobalClientHandler.Check4Add(checkArgs4AddArgs)
	if err != nil {
		logrus.Errorf("fail to check args for add operation. Error detail: %s", err.Error())
		return err
	}
	logrus.Infof("file size is : %v", info.Size())
	logrus.Infof("chunk num is : %v", checkArgs4AddReply.ChunkNum)
	var (
		wg             sync.WaitGroup
		chunkChan      = make(chan *os.File)
		resultChan     = make(chan *util.ChunkSendResult)
		fileNodeId     = checkArgs4AddReply.FileNodeId
		chunkNum       = checkArgs4AddReply.ChunkNum
		goroutineCount int
	)
	goroutineCount = maxGoroutineCount
	if maxGoroutineCount > chunkNum {
		goroutineCount = int(chunkNum)
	}
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumeChunk(chunkChan, resultChan, fileNodeId)
		}()
	}
	for i := 0; i < (int)(chunkNum); i++ {
		file, err := os.Open(src)
		if err != nil {
			logrus.Errorf("fail to open file errr detail : %s", err.Error())
		}
		logrus.Info("Seek index: ", common.ChunkSize*i)
		_, err = file.Seek(int64(common.ChunkSize*i), 0)
		if err != nil {
			logrus.Errorf("Seek fail.Error detail : %s", err.Error())
		}
		chunkChan <- file
		logrus.Infof("file %s add to chunkChan", file.Name())
	}
	close(chunkChan)
	wg.Wait()
	close(resultChan)
	infos := make([]*pb.ChunkInfo4Add, 0, chunkNum)
	failChunkIds := make([]string, 0, chunkNum)
	for result := range resultChan {
		if len(result.SuccessDataNodes) == 0 {
			failChunkIds = append(failChunkIds, result.ChunkId)
			continue
		}
		infos = append(infos, &pb.ChunkInfo4Add{
			ChunkId:     result.ChunkId,
			SuccessNode: result.SuccessDataNodes,
			FailNode:    result.FailDataNodes,
		})
	}
	unlockDic4AddArgs := &pb.UnlockDic4AddArgs{
		FileNodeId:   fileNodeId,
		FilePath:     targetPath + pathSplitString + fileName,
		Infos:        infos,
		FailChunkIds: failChunkIds,
	}
	_, err = GlobalClientHandler.UnlockDic4Add(unlockDic4AddArgs)
	if err != nil {
		logrus.Errorf("fail to unlock FileNodes in the target path, error detail: %s", err.Error())
		return err
	}
	return nil
}

func consumeChunk(chunkChan chan *os.File, resultChan chan *util.ChunkSendResult, fileNodeId string) {
	for file := range chunkChan {
		offset, _ := file.Seek(0, 1)
		index := int32(offset / common.ChunkSize)
		logrus.Infof("offset : %v", offset)
		getDataNodes4AddArgs := &pb.GetDataNodes4AddArgs{
			FileNodeId: fileNodeId,
			ChunkIndex: index,
		}
		getDataNodes4AddReply, err := GlobalClientHandler.GetDataNodes4Add(getDataNodes4AddArgs)
		if err != nil {
			file.Close()
		}
		dataNodeIds := getDataNodes4AddReply.DataNodeIds
		dataNodeAdds := getDataNodes4AddReply.DataNodes
		chunkId := fileNodeId + common.ChunkIdDelimiter + strconv.Itoa(int(index))
		currentResult := &util.ChunkSendResult{
			ChunkId:          chunkId,
			FailDataNodes:    dataNodeIds,
			SuccessDataNodes: dataNodeIds[0:0],
		}
		for i := 0; i < len(getDataNodes4AddReply.DataNodeIds); i++ {
			stream, err := getStream(chunkId, getDataNodes4AddReply)
			if err != nil {
				dataNodeIds = append(dataNodeIds[1:], dataNodeIds[0])
				dataNodeAdds = append(dataNodeAdds[1:], dataNodeAdds[0])
				continue
			}
			for i := 0; i < common.ChunkMBNum; i++ {
				buffer := make([]byte, common.MB)
				n, err := file.Read(buffer)
				if err == io.EOF {
					reply, err := stream.CloseAndRecv()
					if err != nil || len(reply.FailAdds) == len(dataNodeIds) {
						logrus.Errorf("fail to close stream, error detail: %s", err.Error())
						dataNodeIds = append(dataNodeIds[1:], dataNodeIds[0])
						dataNodeAdds = append(dataNodeAdds[1:], dataNodeAdds[0])
						continue
					}
					currentResult = util.ConvReply2SingleResult(reply, dataNodeIds, dataNodeAdds)
					break
				}
				err = stream.Send(&pb.PieceOfChunk{
					Piece: buffer[:n],
				})
				if err != nil {
					logrus.Errorf("fail to send a piece to primary chunkserver, error detail: %s", err.Error())
					dataNodeIds = append(dataNodeIds[1:], dataNodeIds[0])
					dataNodeAdds = append(dataNodeAdds[1:], dataNodeAdds[0])
					continue
				}
				if i == common.ChunkMBNum-1 {
					reply, err := stream.CloseAndRecv()
					if err != nil || len(reply.FailAdds) == len(dataNodeIds) {
						logrus.Errorf("fail to close stream, error detail: %s", err.Error())
						dataNodeIds = append(dataNodeIds[1:], dataNodeIds[0])
						dataNodeAdds = append(dataNodeAdds[1:], dataNodeAdds[0])
						continue
					}
					currentResult = util.ConvReply2SingleResult(reply, dataNodeIds, dataNodeAdds)
				}
			}
			file.Close()
			break
		}
		resultChan <- currentResult
	}
}

// getStream Build stream to transfer this chunk to primary chunkserver.
func getStream(chunkId string, args *pb.GetDataNodes4AddReply) (pb.PipLineService_TransferChunkClient, error) {
	// Todo DataNodes may be empty.
	conn, _ := grpc.Dial(args.DataNodes[0]+common.AddressDelimiter+viper.GetString(common.ChunkPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	c := pb.NewPipLineServiceClient(conn)
	newCtx := context.Background()
	for _, address := range args.DataNodes {
		newCtx = metadata.AppendToOutgoingContext(newCtx, common.AddressString, address)
	}
	newCtx = metadata.AppendToOutgoingContext(newCtx, common.ChunkIdString, chunkId)
	return c.TransferChunk(newCtx)
}
