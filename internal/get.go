package internal

import (
	"fmt"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/viper"
	"golang.org/x/sys/unix"
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
	for i := 0; i < chunkNum; i++ {
		//TODO 写文件是单句柄还是多句柄
		file, err := os.OpenFile(des, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0755)
		// Make sure all the file is closed
		if err != nil {
			file.Close()
			errChan <- err
			break
		}
		currentChunkSize := int64(common.ChunkSize)
		if i == chunkNum-1 {
			currentChunkSize = int64(fileSize % common.ChunkSize)
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
			dataNodeIds      = getDataNodes4GetReply.DataNodeIds
			dataNodeAddrs    = getDataNodes4GetReply.DataNodeAddrs
			primaryNodeIndex = 0
			chunkId          = fileNodeId + common.ChunkIdDelimiter + strconv.FormatInt(int64(info.chunkIndex), 10)
			buffer, _        = unix.Mmap(int(info.file.Fd()), int64(info.chunkIndex*common.ChunkSize),
				int(info.currentChunkSize), unix.PROT_WRITE, unix.MAP_SHARED)
		)
		Logger.Debugf("Start getting data with chunk %s", chunkId)
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
				errChan <- fmt.Errorf("all of dataNode's file is ruined.FileNodeId = %s", fileNodeId)
				_ = unix.Munmap(buffer)
				info.file.Close()
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
					Logger.Debugf("Chunk %d write done!", info.chunkIndex)
					break
				} else {
					errChan <- err
				}
			}
			buffer = pieceOfChunk.Piece
			/*if _, err := info.file.Write(pieceOfChunk.Piece); err != nil {
				Logger.Errorf("Fail to write a piece to chunk file, error detail: %s", err.Error())
				errChan <- err
			}*/
		}
		bar.Add(1)
		_ = unix.Munmap(buffer)
		info.file.Close()
	}
}
