package internal

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
	"log"
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
	_, err := os.Stat(des)
	if err == nil {
		return fmt.Errorf("File exists.\n")
	}
	file, err = os.OpenFile(des, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0755)
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
		wg             = &sync.WaitGroup{}
		indexChan      = make(chan int)
		errChan        = make(chan error)
		goroutineCount int
	)
	goroutineCount = maxGoroutineCount
	if maxGoroutineCount > chunkNum {
		goroutineCount = int(chunkNum)
	}
	// listen to the rpc port when client is ready to receive data
	//go GlobalClientHandler.Server()
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go produce(fileNodeId, indexChan, errChan, wg, file)
	}
	for i := 0; i < int(chunkNum); i++ {
		indexChan <- i
	}
	close(indexChan)
	wg.Wait()
	close(errChan)
	if len(errChan) != 0 {
		log.Println("Print all errors")
		for e := range errChan {
			log.Println(e)
		}
		return <-errChan
	}
	return nil
}

func produce(fileNodeId string, index chan int, errChan chan error, wg *sync.WaitGroup, file *os.File) {
	defer wg.Done()
	log.Println("Produce method with fileNodeId ", fileNodeId)
	for chunkIndex := range index {
		getDataNodes4GetArgs := &pb.GetDataNodes4GetArgs{
			FileNodeId: fileNodeId,
			ChunkIndex: int32(chunkIndex),
		}
		getDataNodes4GetReply, err := GlobalClientHandler.GetDataNodes4Get(getDataNodes4GetArgs)
		if err != nil {
			errChan <- err
			//TODO break的正确性
			break
		}
		var (
			dataNodeIds      = getDataNodes4GetReply.DataNodeIds
			dataNodeAddrs    = getDataNodes4GetReply.DataNodeAddrs
			primaryNodeIndex = 0
			chunkId          = fileNodeId + common.ChunkIdDelimiter + strconv.Itoa(chunkIndex)
		)
		log.Println("3. call SetupStream2DataNode rpc with chunkId ", chunkId)
		setupStream2DataNodeArgs := &pb.SetupStream2DataNodeArgs{
			ClientPort: viper.GetString(common.ClientPort),
			ChunkId:    chunkId,
			DataNodeId: dataNodeIds[primaryNodeIndex],
		}
		//TODO 在建立stream连接前，需要先在master处将对应的dataNode的lease++
		//TODO 错误重试
		stream, err := GlobalClientHandler.SetupStream2DataNode(
			dataNodeAddrs[primaryNodeIndex], setupStream2DataNodeArgs)
		// if primary datanode fails, client will try to connect the next datanode
		for err != nil {
			primaryNodeIndex++
			if primaryNodeIndex >= len(dataNodeAddrs) {
				errChan <- fmt.Errorf("All of dataNode's file is ruined.FileNodeId = %s\n", fileNodeId)
				//TODO return的正确性
				return
			}
			stream, err = GlobalClientHandler.SetupStream2DataNode(
				dataNodeAddrs[primaryNodeIndex], setupStream2DataNodeArgs)
		}
		_, err = file.Seek(int64(common.ChunkSize*chunkIndex), 0)
		if err != nil {
			log.Println("file.Seek error ", err)
			return
		}
		// Receive pieces of chunk until there are no more pieces
		for {
			pieceOfChunk, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					err = stream.CloseSend()
					if err != nil {
						logrus.Errorf("fail to close receive stream, error detail: %s", err.Error())
						errChan <- err
					}
					logrus.Infof("%d write done!\n", chunkIndex)
					break
				} else {
					errChan <- err
				}
			}
			storeFile(pieceOfChunk, int32(chunkIndex))
		}
	}
}

func storeFile(piece *pb.Piece, index int32) {
	// Goroutine will be blocked until main thread receive pieces of chunk and put them into pieceChan
	if _, err := file.Write(piece.Piece); err != nil {
		logrus.Errorf("fail to write a piece to chunk file, error detail: %s\n", err.Error())

	}

}
