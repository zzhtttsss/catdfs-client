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
		logrus.Errorf("fail to check args for add operation, error detail: %s", err.Error())
		return err
	}
	logrus.Infof("file size is : %v", info.Size())
	logrus.Infof("chunk num is : %v", checkArgs4AddReply.ChunkNum)
	var (
		wg             sync.WaitGroup
		chunkChan      = make(chan *os.File)
		errChan        = make(chan error)
		fileNodeId     = checkArgs4AddReply.FileNodeId
		goroutineCount int
	)
	goroutineCount = maxGoroutineCount
	if maxGoroutineCount > checkArgs4AddReply.ChunkNum {
		goroutineCount = int(checkArgs4AddReply.ChunkNum)
	}
	logrus.Infof("goroutineCount is : %v", goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumeChunk(chunkChan, errChan, fileNodeId)
		}()
	}
	for i := 0; i < (int)(checkArgs4AddReply.ChunkNum); i++ {
		file, err := os.Open(src)
		if err != nil {
			logrus.Errorf("fail to open file errr detail : %s", err.Error())
		}
		file.Seek(int64(common.ChunkSize*i), 0)
		chunkChan <- file
	}
	close(chunkChan)
	wg.Wait()
	close(errChan)
	if len(errChan) != 0 {
		return <-errChan
	}
	return nil
}

func consumeChunk(chunkChan chan *os.File, errChan chan error, fileNodeId string) {
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
			errChan <- err
			file.Close()
		}
		chunkId := fileNodeId + common.ChunkIdDelimiter + strconv.Itoa(int(index))
		stream, err := getStream(chunkId, getDataNodes4AddReply)
		for i := 0; i < 64; i++ {
			offset, _ := file.Seek(0, 1)
			logrus.Infof("offset : %v", offset)
			buffer := make([]byte, common.MB)
			n, err := file.Read(buffer)
			if err == io.EOF {
				logrus.Infof("EOF!")
				_, err = stream.CloseAndRecv()
				logrus.Infof("close!")
				if err != nil {
					logrus.Errorf("fail to close stream, error detail: %s", err.Error())
					errChan <- err
				}
				break
			}
			if stream == nil {
				logrus.Info("stream is nil!")
			}
			err = stream.Send(&pb.PieceOfChunk{
				Piece: buffer[:n],
			})
			if err != nil {
				logrus.Errorf("fail to send a piece to primary chunkserver, error detail: %s", err.Error())
				errChan <- err
			}
			offset, _ = file.Seek(0, 1)
			logrus.Infof("after offset : %v", offset)

		}
		file.Close()
	}
}

// getStream Build stream to transfer this chunk to primary chunkserver.
func getStream(chunkId string, args *pb.GetDataNodes4AddReply) (pb.PipLineService_TransferChunkClient, error) {
	conn, _ := grpc.Dial(args.PrimaryNode+common.AddressDelimiter+viper.GetString(common.ChunkPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	c := pb.NewPipLineServiceClient(conn)
	newCtx := context.Background()
	for _, address := range args.DataNodes {
		newCtx = metadata.AppendToOutgoingContext(newCtx, common.AddressString, address)
	}
	newCtx = metadata.AppendToOutgoingContext(newCtx, common.ChunkIdString, chunkId)
	return c.TransferChunk(newCtx)
}