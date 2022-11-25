package internal

import (
	"fmt"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/viper"
	"golang.org/x/sys/unix"
	"io"
	"math"
	"os"
	"strconv"
	"sync"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

const (
	FileSplitChar = '/'
)

type ChunkGetInfo struct {
	file             *os.File
	chunkIndex       int
	currentChunkSize int64
}

func Get(src, des string) error {
	Logger.Infof("Start to get a file, src: %s, des: %s", src, des)
	if src[0] != FileSplitChar || src[len(src)-1] == FileSplitChar {
		return fmt.Errorf("get the wrong path: %s", src)
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
		chunkNum   = int(checkAndGetReply.ChunkNum)
		fileNodeId = checkAndGetReply.FileNodeId
		fileSize   = checkAndGetReply.FileSize
	)
	bar = progressbar.NewOptions64(int64(chunkNum), progressbar.OptionSetDescription("Downloading..."),
		progressbar.OptionEnableColorCodes(true), progressbar.OptionSetItsString("Chunks"),
		progressbar.OptionShowIts(), progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))
	Logger.Debugf("Find file with fileNode %s and chunk num %v.", fileNodeId, chunkNum)
	var (
		wg             = &sync.WaitGroup{}
		fileChan       = make(chan *ChunkGetInfo)
		errChan        = make(chan error, chunkNum)
		goroutineCount int
	)
	goroutineCount = maxGoroutineCount
	if maxGoroutineCount > chunkNum {
		goroutineCount = chunkNum
	}
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go produce(fileNodeId, fileChan, errChan, wg)
	}
	file, err := os.OpenFile(des, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		closeResource(file, fileChan, errChan)
		return err
	}
	err = file.Truncate(fileSize)
	if err != nil {
		closeResource(file, fileChan, errChan)
		return err
	}
	for i := 0; i < chunkNum; i++ {
		currentChunkSize := int64(common.ChunkSize)
		if i == chunkNum-1 {
			currentChunkSize = fileSize % common.ChunkSize
			if currentChunkSize == 0 {
				currentChunkSize = common.ChunkSize
			}
		}
		fileChan <- &ChunkGetInfo{
			file:             file,
			chunkIndex:       i,
			currentChunkSize: currentChunkSize,
		}
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

func produce(fileNodeId string, fileChan chan *ChunkGetInfo, errChan chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for info := range fileChan {
		Logger.Debugf("ready to write file with index %d", info.chunkIndex)
		getDataNodes4GetArgs := &pb.GetDataNodes4GetArgs{
			FileNodeId: fileNodeId,
			ChunkIndex: int32(info.chunkIndex),
		}
		getDataNodes4GetReply, err := GlobalClientHandler.GetDataNodes4Get(getDataNodes4GetArgs)
		if err != nil {
			errChan <- err
			info.file.Close()
			break
		}
		var (
			dataNodeIds   = getDataNodes4GetReply.DataNodeIds
			dataNodeAddrs = getDataNodes4GetReply.DataNodeAddrs
			chunkId       = fileNodeId + common.ChunkIdDelimiter + strconv.FormatInt(int64(info.chunkIndex), 10)
			buffer, _     = unix.Mmap(int(info.file.Fd()), int64(info.chunkIndex*common.ChunkSize),
				int(info.currentChunkSize), unix.PROT_WRITE, unix.MAP_SHARED)
			pieceNumber = int(math.Ceil(float64(info.currentChunkSize) / float64(common.MB)))
			pieceIndex  = 0
		)
		Logger.Debugf("Start getting data with chunk %s", chunkId)
		stream, err := getAvailableStream(dataNodeAddrs, dataNodeIds, chunkId)
		if err != nil {
			errChan <- err
			_ = unix.Munmap(buffer)
			info.file.Close()
			return
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
					Logger.Debugf("Chunk %d write done!", info.chunkIndex)
					break
				} else {
					errChan <- err
				}
			}
			byteIndex := pieceIndex * common.MB
			lastPieceSize := common.MB
			if pieceIndex == pieceNumber-1 {
				lastPieceSize = int(info.currentChunkSize % common.MB)
				if lastPieceSize == 0 {
					lastPieceSize = common.MB
				}
			}
			for i := byteIndex; i < byteIndex+lastPieceSize; i++ {
				buffer[i] = pieceOfChunk.Piece[i-byteIndex]
			}
			pieceIndex++
		}
		bar.Add(1)
		_ = unix.Munmap(buffer)
		info.file.Close()
	}
}

func getAvailableStream(dataNodeAddrs, dataNodeIds []string, chunkId string) (pb.SetupStream_SetupStream2DataNodeClient, error) {
	primaryNodeIndex := 0
	setupStream2DataNodeArgs := &pb.SetupStream2DataNodeArgs{
		ClientPort: viper.GetString(common.ClientPort),
		ChunkId:    chunkId,
		DataNodeId: dataNodeIds[primaryNodeIndex],
	}
	stream, err := GlobalClientHandler.SetupStream2DataNode(
		dataNodeAddrs[primaryNodeIndex], setupStream2DataNodeArgs)
	// if primary datanode fails, client will try to connect the next datanode
	for err != nil {
		primaryNodeIndex++
		if primaryNodeIndex >= len(dataNodeAddrs) {
			return nil, fmt.Errorf("all of dataNode's file is ruined.ChunkId = %s", chunkId)
		}
		stream, err = GlobalClientHandler.SetupStream2DataNode(
			dataNodeAddrs[primaryNodeIndex], setupStream2DataNodeArgs)
	}
	return stream, nil
}

func closeResource(fileResource *os.File, fileChan chan *ChunkGetInfo, errChan chan error) {
	if fileResource != nil {
		_ = fileResource.Close()
	}
	if fileChan != nil {
		close(fileChan)
	}
	if errChan != nil {
		close(errChan)
	}
}
