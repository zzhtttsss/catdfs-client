package internal

import (
	"fmt"
	"github.com/spf13/viper"
	"io"
	"os"
	"strconv"
	"sync"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

const (
	FileSplitChar = '/'
)

func Get(src, des string) error {
	Logger.Infof("Start to get a file, src: %s, des: %s", src, des)
	if src[0] != FileSplitChar || src[len(src)-1] == FileSplitChar {
		return fmt.Errorf("Get the wrong path: %s", src)
	}
	_, err := os.Stat(des)
	if err == nil {
		return fmt.Errorf("file exists")
	}

	checkAndGetArgs := &pb.CheckAndGetArgs{Path: src}
	checkAndGetReply, err := GlobalClientHandler.CheckAndGet(checkAndGetArgs)
	if err != nil {
		return err
	}
	var (
		chunkNum   = checkAndGetReply.ChunkNum
		fileNodeId = checkAndGetReply.FileNodeId
	)
	Logger.Debugf("file node id is : %v", fileNodeId)
	Logger.Debugf("chunk num is : %v", chunkNum)
	var (
		wg             = &sync.WaitGroup{}
		fileChan       = make(chan *os.File)
		errChan        = make(chan error)
		goroutineCount int
	)
	goroutineCount = maxGoroutineCount
	if maxGoroutineCount > chunkNum {
		goroutineCount = int(chunkNum)
	}
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go produce(fileNodeId, fileChan, errChan, wg)
	}
	for i := 0; i < int(chunkNum); i++ {
		file, err := os.OpenFile(des, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0755)
		// Make sure all the file is closed
		if err != nil {
			file.Close()
			errChan <- err
		}
		file.Seek(int64(common.ChunkSize*i), 0)
		fileChan <- file
	}
	close(fileChan)
	wg.Wait()
	close(errChan)
	if len(errChan) != 0 {
		return <-errChan
	}
	Logger.Infof("Success to get a file, src: %s, des: %s", src, des)
	return nil
}

func produce(fileNodeId string, fileChan chan *os.File, errChan chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for file := range fileChan {
		offset, _ := file.Seek(0, 1)
		index := int32(offset / common.ChunkSize)
		Logger.Debugf("offset : %v in index %v", offset, index)
		getDataNodes4GetArgs := &pb.GetDataNodes4GetArgs{
			FileNodeId: fileNodeId,
			ChunkIndex: index,
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
			chunkId          = fileNodeId + common.ChunkIdDelimiter + strconv.FormatInt(int64(index), 10)
		)
		setupStream2DataNodeArgs := &pb.SetupStream2DataNodeArgs{
			ClientPort: viper.GetString(common.ClientPort),
			ChunkId:    chunkId,
			DataNodeId: dataNodeIds[primaryNodeIndex],
		}
		//TODO 在建立stream连接前，需要先在master处将对应的dataNode的lease++
		stream, err := GlobalClientHandler.SetupStream2DataNode(
			dataNodeAddrs[primaryNodeIndex], setupStream2DataNodeArgs)
		// if primary datanode fails, client will try to connect the next datanode
		for err != nil {
			primaryNodeIndex++
			if primaryNodeIndex >= len(dataNodeAddrs) {
				errChan <- fmt.Errorf("all of dataNode's file is ruined.FileNodeId = %s", fileNodeId)
				file.Close()
				//TODO return的正确性
				return
			}
			stream, err = GlobalClientHandler.SetupStream2DataNode(
				dataNodeAddrs[primaryNodeIndex], setupStream2DataNodeArgs)
		}
		// Receive pieces of chunk until there are no more pieces
		for {
			pieceOfChunk, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					err = stream.CloseSend()
					if err != nil {
						Logger.Errorf("Fail to close receive stream, error detail: %s", err.Error())
						errChan <- err
					}
					Logger.Debugf("Chunk %d write done!", index)
					file.Close()
					break
				} else {
					errChan <- err
				}
			}
			if _, err := file.Write(pieceOfChunk.Piece); err != nil {
				Logger.Errorf("Fail to write a piece to chunk file, error detail: %s", err.Error())

			}
		}
	}
}
