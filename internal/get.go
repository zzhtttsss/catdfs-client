package internal

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
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
	if _, err := os.Stat(des); err == nil {
		return fmt.Errorf("File exists.\n")
	}
	file, err := os.OpenFile(des, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0755)
	defer file.Close()
	if err != nil {
		return err
	}
	log.Println("1. call CheckAndGet rpc")
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
	go GlobalClientHandler.Server()
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
		return <-errChan
	}
	return nil
}

func produce(fileNodeId string, index chan int, errChan chan error, wg *sync.WaitGroup, file *os.File) {
	defer wg.Done()
	log.Println("Produce method with fileNodeId ", fileNodeId)
	for chunkIndex := range index {
		log.Println("2. call GetDataNodes4Get rpc with index ", chunkIndex)
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
		log.Println("3. call SetupStream2DataNode rpc with chunkId ", chunkId)
		setupStream2DataNodeArgs := &pb.SetupStream2DataNodeArgs{
			ClientPort: viper.GetString(common.ChunkPort),
			ChunkId:    chunkId,
			DataNodeId: dataNodeIds[primaryNodeIndex],
		}
		//TODO 在建立stream连接前，需要先在master处将对应的dataNode的lease++
		_, err = GlobalClientHandler.SetupStream2DataNode(
			dataNodeAddrs[primaryNodeIndex], setupStream2DataNodeArgs)
		// if primary datanode fails, client will try to connect the next datanode
		for err != nil {
			log.Println(err)
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
