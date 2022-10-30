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
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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
		logrus.Errorf("Fail to check args for add operation. Error detail: %s", err.Error())
		return err
	}
	logrus.Infof("file size is : %v", info.Size())
	logrus.Infof("chunk num is : %v", checkArgs4AddReply.ChunkNum)
	var (
		wg             sync.WaitGroup
		chunkChan      = make(chan *ChunkAddInfo)
		fileNodeId     = checkArgs4AddReply.FileNodeId
		chunkNum       = checkArgs4AddReply.ChunkNum
		resultChan     = make(chan *util.ChunkSendResult, chunkNum)
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
	getDataNodes4AddArgs := &pb.GetDataNodes4AddArgs{
		FileNodeId: fileNodeId,
		ChunkNum:   chunkNum,
	}
	reply, err := GlobalClientHandler.GetDataNodes4Add(getDataNodes4AddArgs)
	for i := 0; i < (int)(chunkNum); i++ {
		file, err := os.Open(src)
		if err != nil {
			logrus.Errorf("Fail to open file error detail : %s", err.Error())
		}
		logrus.Info("Seek index: ", common.ChunkSize*i)
		_, err = file.Seek(int64(common.ChunkSize*i), 0)
		if err != nil {
			logrus.Errorf("Fail to seek chunk %v of %s, Error detail : %s", i, file.Name(), err.Error())
		}

		chunkChan <- &ChunkAddInfo{
			file:         file,
			dataNodeIds:  reply.DataNodeIds[i].Items,
			dataNodeAdds: reply.DataNodeAdds[i].Items,
		}
		logrus.Infof("Chunk %v of %s add to chunkChan", i, file.Name())
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
		logrus.Errorf("Fail to unlock FileNodes in the target path, error detail: %s", err.Error())
		return err
	}
	return nil
}

type ChunkAddInfo struct {
	file         *os.File
	dataNodeIds  []string
	dataNodeAdds []string
}

// consumeChunk get ChunkAddInfo from chunkChan and establish a pipeline to send
// a Chunk to all target DataNode.
func consumeChunk(chunkChan chan *ChunkAddInfo, resultChan chan *util.ChunkSendResult, fileNodeId string) {
	for info := range chunkChan {
		offset, _ := info.file.Seek(0, 1)
		index := int32(offset / common.ChunkSize)
		// Sometimes a DataNode may be allocated to store multiple Chunk of a file, and it may be the
		// first DataNode to receive data from client. If this DataNode crashes during transferring
		// data, the client will need to re-establish multiple pipelines. So client will shuffle the
		// order of DataNode in each pipeline.
		dataNodeIds, dataNodeAdds := randShuffle(info.dataNodeIds, info.dataNodeAdds)
		logrus.Infof("Get datanodes, chunk id: %v, datanode ids: %v, datanode addresses: %v",
			index, info.dataNodeIds, info.dataNodeAdds)
		chunkId := fileNodeId + common.ChunkIdDelimiter + strconv.Itoa(int(index))
		currentResult := &util.ChunkSendResult{
			ChunkId:          chunkId,
			FailDataNodes:    dataNodeIds,
			SuccessDataNodes: dataNodeIds[0:0],
		}
		isSuccess := false
		for i := 0; i < len(dataNodeIds); i++ {
			logrus.Infof("Chunk %v try %v datanode, id: %s, address: %s", index, i, dataNodeIds[0], dataNodeAdds[0])
			stream, err := getStream(chunkId, dataNodeAdds)
			if err != nil {
				dataNodeIds = append(dataNodeIds[1:], dataNodeIds[0])
				dataNodeAdds = append(dataNodeAdds[1:], dataNodeAdds[0])
				continue
			}
			for i := 0; i < common.ChunkMBNum; i++ {
				buffer := make([]byte, common.MB)
				n, err := info.file.Read(buffer)
				if err == io.EOF {
					reply, err := stream.CloseAndRecv()
					if err != nil || len(reply.FailAdds) == len(dataNodeIds) {
						logrus.Errorf("Fail to close stream, error detail: %s", err.Error())
						dataNodeIds = append(dataNodeIds[1:], dataNodeIds[0])
						dataNodeAdds = append(dataNodeAdds[1:], dataNodeAdds[0])
						break
					}
					currentResult = util.ConvReply2SingleResult(reply, dataNodeIds, dataNodeAdds, common.Add)
					isSuccess = true
					currentResult = util.ConvReply2SingleResult(reply, dataNodeIds, dataNodeAdds, common.Copy)
					break
				}
				err = stream.Send(&pb.PieceOfChunk{
					Piece: buffer[:n],
				})
				if err != nil {
					logrus.Errorf("Fail to send a piece to primary chunkserver, error detail: %s", err.Error())
					dataNodeIds = append(dataNodeIds[1:], dataNodeIds[0])
					dataNodeAdds = append(dataNodeAdds[1:], dataNodeAdds[0])
					continue
				}
				if i == common.ChunkMBNum-1 {
					reply, err := stream.CloseAndRecv()
					if err != nil || len(reply.FailAdds) == len(dataNodeIds) {
						logrus.Errorf("Fail to close stream, error detail: %s", err.Error())
						dataNodeIds = append(dataNodeIds[1:], dataNodeIds[0])
						dataNodeAdds = append(dataNodeAdds[1:], dataNodeAdds[0])
						continue
					}
					currentResult = util.ConvReply2SingleResult(reply, dataNodeIds, dataNodeAdds, common.Add)
					isSuccess = true
					currentResult = util.ConvReply2SingleResult(reply, dataNodeIds, dataNodeAdds, common.Copy)
				}
			}
			info.file.Close()
			if isSuccess {
				break
			}
		}
		resultChan <- currentResult
	}
}

// getStream Build stream to transfer this chunk to primary chunkserver.
func getStream(chunkId string, dataNodeAdds []string) (pb.PipLineService_TransferChunkClient, error) {
	// Todo DataNodes may be empty.
	nextAddress := dataNodeAdds[0]
	logrus.Infof("Get stream, chunk id: %s, next address: %s", chunkId, nextAddress)
	conn, _ := grpc.Dial(nextAddress+common.AddressDelimiter+viper.GetString(common.ChunkPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	c := pb.NewPipLineServiceClient(conn)
	newCtx := context.Background()
	for _, address := range dataNodeAdds {
		newCtx = metadata.AppendToOutgoingContext(newCtx, common.AddressString, address)
	}
	newCtx = metadata.AppendToOutgoingContext(newCtx, common.ChunkIdString, chunkId)
	return c.TransferChunk(newCtx)
}

// randShuffle randomly shuffles the two slices given.
func randShuffle(dataNodeIds []string, dataNodeAdds []string) ([]string, []string) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(dataNodeIds), func(i, j int) {
		dataNodeIds[i], dataNodeIds[j] = dataNodeIds[j], dataNodeIds[i]
		dataNodeAdds[i], dataNodeAdds[j] = dataNodeAdds[j], dataNodeAdds[i]
	})
	return dataNodeIds, dataNodeAdds
}
