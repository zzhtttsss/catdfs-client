package internal

import (
	"context"
	"fmt"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/viper"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"math"
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

var bar *progressbar.ProgressBar

func Add(src, des string) error {
	Logger.Infof("Start to add a file, src: %s, des: %s", src, des)
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
		srcPath := strings.Split(src, pathSplitString)
		srcPathLength := len(srcPath)
		fileName = srcPath[srcPathLength-1]
		targetPath = des
	} else {
		fileName = desPath[desPathLength-1]
		targetPath = strings.Join(desPath[:desPathLength-1], pathSplitString)
	}
	Logger.Debugf("Check for file %s, size %d", fileName, info.Size())
	checkArgs4AddArgs := &pb.CheckArgs4AddArgs{
		Path:     targetPath,
		FileName: fileName,
		Size:     info.Size(),
	}
	checkArgs4AddReply, err := GlobalClientHandler.Check4Add(checkArgs4AddArgs)
	if err != nil {
		Logger.Errorf("Fail to check args for add operation. Error detail: %s", err.Error())
		return err
	}
	Logger.Debugf("file size is : %v", info.Size())
	Logger.Debugf("chunk num is : %v", checkArgs4AddReply.ChunkNum)
	var (
		wg             sync.WaitGroup
		chunkChan      = make(chan *ChunkAddInfo)
		fileNodeId     = checkArgs4AddReply.FileNodeId
		chunkNum       = checkArgs4AddReply.ChunkNum
		resultChan     = make(chan *util.ChunkSendResult, chunkNum)
		goroutineCount int
	)
	bar = progressbar.NewOptions64(int64(chunkNum), progressbar.OptionSetDescription("Uploading..."),
		progressbar.OptionEnableColorCodes(true), progressbar.OptionSetItsString("Chunks"),
		progressbar.OptionShowIts(), progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	goroutineCount = maxGoroutineCount
	if maxGoroutineCount > chunkNum {
		goroutineCount = int(chunkNum)
	}
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumeChunk(chunkChan, resultChan, fileNodeId, info.Size())
		}()
	}
	getDataNodes4AddArgs := &pb.GetDataNodes4AddArgs{
		FileNodeId: fileNodeId,
		ChunkNum:   chunkNum,
	}
	reply, err := GlobalClientHandler.GetDataNodes4Add(getDataNodes4AddArgs)
	for i := 0; i < (int)(chunkNum); i++ {
		file, err := os.OpenFile(src, os.O_RDWR, 0644)
		if err != nil {
			close(chunkChan)
			close(resultChan)
			return err
		}
		chunkChan <- &ChunkAddInfo{
			file:         file,
			chunkIndex:   i,
			isLast:       i == (int)(chunkNum)-1,
			dataNodeIds:  reply.DataNodeIds[i].Items,
			dataNodeAdds: reply.DataNodeAdds[i].Items,
		}
		Logger.Debugf("Chunk %v of %s add to chunkChan", i, file.Name())
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
	unlockDic4AddArgs := &pb.Callback4AddArgs{
		FileNodeId:   fileNodeId,
		FilePath:     targetPath + pathSplitString + fileName,
		Infos:        infos,
		FailChunkIds: failChunkIds,
	}
	_, err = GlobalClientHandler.Callback4Add(unlockDic4AddArgs)
	if err != nil {
		return err
	}
	Logger.Infof("Success to add a file, src: %s, des: %s", src, des)
	return nil
}

type ChunkAddInfo struct {
	file         *os.File
	chunkIndex   int
	isLast       bool
	dataNodeIds  []string
	dataNodeAdds []string
}

// consumeChunk get ChunkAddInfo from chunkChan and establish a pipeline to send
// a Chunk to all target DataNode.
func consumeChunk(chunkChan chan *ChunkAddInfo, resultChan chan *util.ChunkSendResult, fileNodeId string,
	fileSize int64) {
	for info := range chunkChan {
		var (
			index         = info.chunkIndex
			lastPieceSize int
			pieceNum      int
			chunkSize     int
			isSuccess     = false
		)
		// Sometimes a DataNode may be allocated to store multiple Chunk of a file, and it may be the
		// first DataNode to receive data from client. If this DataNode crashes during transferring
		// data, the client will need to re-establish multiple pipelines. So client will shuffle the
		// order of DataNode in each pipeline.
		dataNodeIds, dataNodeAdds := randShuffle(info.dataNodeIds, info.dataNodeAdds)
		Logger.Debugf("Get datanodes, chunk id: %v, datanode ids: %v, datanode addresses: %v",
			index, info.dataNodeIds, info.dataNodeAdds)
		chunkId := fileNodeId + common.ChunkIdDelimiter + strconv.Itoa(index)
		currentResult := &util.ChunkSendResult{
			ChunkId:          chunkId,
			FailDataNodes:    dataNodeIds,
			SuccessDataNodes: dataNodeIds[0:0],
		}
		if info.isLast {
			chunkSize = int(fileSize % common.ChunkSize)
			if chunkSize == 0 {
				chunkSize = common.ChunkSize
			}
			pieceNum = int(math.Ceil(float64(chunkSize) / float64(common.MB)))
			lastPieceSize = chunkSize % common.MB
		} else {
			chunkSize = common.ChunkSize
			pieceNum = common.ChunkMBNum
			lastPieceSize = common.MB
		}

		buffer, _ := unix.Mmap(int(info.file.Fd()), int64(index*common.ChunkSize), chunkSize,
			unix.PROT_WRITE, unix.MAP_SHARED)
		for i := 0; i < len(dataNodeIds); i++ {
			Logger.Debugf("Chunk %v try %v datanode, id: %s, address: %s", index, i, dataNodeIds[0], dataNodeAdds[0])
			stream, err := getStream(chunkId, dataNodeAdds, chunkSize)
			if err != nil {
				dataNodeIds = append(dataNodeIds[1:], dataNodeIds[0])
				dataNodeAdds = append(dataNodeAdds[1:], dataNodeAdds[0])
				continue
			}

			for j := 0; j < pieceNum; j++ {
				byteIndex := j * common.MB
				if j == pieceNum-1 {
					err = stream.Send(&pb.PieceOfChunk{
						Piece: buffer[byteIndex : byteIndex+lastPieceSize],
					})
				} else {
					err = stream.Send(&pb.PieceOfChunk{
						Piece: buffer[byteIndex : byteIndex+common.MB],
					})
				}
				if err != nil {
					Logger.Errorf("Fail to send a piece to primary chunkserver, error detail: %s", err.Error())
					dataNodeIds = append(dataNodeIds[1:], dataNodeIds[0])
					dataNodeAdds = append(dataNodeAdds[1:], dataNodeAdds[0])
					break
				}

				if j == pieceNum-1 {
					reply, err := stream.CloseAndRecv()
					if err != nil || len(reply.FailAdds) == len(dataNodeIds) {
						Logger.Errorf("Fail to close stream, error detail: %s", err.Error())
						dataNodeIds = append(dataNodeIds[1:], dataNodeIds[0])
						dataNodeAdds = append(dataNodeAdds[1:], dataNodeAdds[0])
						break
					}
					currentResult = util.ConvReply2SingleResult(reply, dataNodeIds, dataNodeAdds, common.AddSendType)
					isSuccess = true
				}
			}
			if isSuccess {
				break
			}
		}
		info.file.Close()
		resultChan <- currentResult
		_ = unix.Munmap(buffer)
		bar.Add(1)
	}
}

// getStream Build stream to transfer this chunk to primary chunkserver.
func getStream(chunkId string, dataNodeAdds []string, chunkSize int) (pb.PipLineService_TransferChunkClient, error) {
	// Todo DataNodes may be empty.
	nextAddress := dataNodeAdds[0]
	Logger.Debugf("Get stream, chunk id: %s, next address: %s", chunkId, nextAddress)
	conn, _ := grpc.Dial(nextAddress+common.AddressDelimiter+viper.GetString(common.ChunkPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	c := pb.NewPipLineServiceClient(conn)
	newCtx := context.Background()
	for _, address := range dataNodeAdds {
		newCtx = metadata.AppendToOutgoingContext(newCtx, common.AddressString, address)
	}
	newCtx = metadata.AppendToOutgoingContext(newCtx, common.ChunkIdString, chunkId)
	newCtx = metadata.AppendToOutgoingContext(newCtx, common.ChunkSizeString, strconv.Itoa(chunkSize))
	return c.TransferChunk(newCtx)
}

// randShuffle randomly shuffles the two slices.
func randShuffle(dataNodeIds []string, dataNodeAdds []string) ([]string, []string) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(dataNodeIds), func(i, j int) {
		dataNodeIds[i], dataNodeIds[j] = dataNodeIds[j], dataNodeIds[i]
		dataNodeAdds[i], dataNodeAdds[j] = dataNodeAdds[j], dataNodeAdds[i]
	})
	return dataNodeIds, dataNodeAdds
}
