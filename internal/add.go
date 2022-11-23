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
	pathSplitString    = "/"
	maxGoroutineCount  = 5
	barDescription4Add = "Uploading..."
	barItsString4Add   = "chunks"
)

var bar *progressbar.ProgressBar

func Add(src, des string) error {
	Logger.Infof("Start to add a file, src: %s, des: %s", src, des)
	// convert the input.
	info, err := getFileAddInfo(src, des)
	if err != nil {
		Logger.Errorf("Fail to check args for add operation. Error detail: %s", err.Error())
		return err
	}

	bar = util.GetProgressBar(int64(info.chunkNum), barDescription4Add, barItsString4Add)

	wg := startConsumeTasks(info)
	err = sendTasks2Consumer(info, src)
	if err != nil {
		Logger.Errorf("Fail to send tasks for consuming. Error detail: %s", err.Error())
		return err
	}

	close(info.addTaskChan)
	wg.Wait()
	close(info.resultChan)

	err = callback2Master(info)
	if err != nil {
		return err
	}
	Logger.Infof("Success to add a file, src: %s, des: %s", src, des)
	return nil
}

// FileAddInfo represents information of adding a file.
type FileAddInfo struct {
	fileName    string
	fileNodeId  string
	targetPath  string
	fileSize    int64
	chunkNum    int
	addTaskChan chan *ChunkAddTask
	resultChan  chan *util.ChunkTaskResult
}

// getFileAddInfo checks input and convert it to FileAddInfo.
func getFileAddInfo(src string, des string) (*FileAddInfo, error) {
	fileName, targetPath, fileSize, err := convertInput4Add(src, des)
	if err != nil {
		return nil, err
	}
	Logger.Debugf("Check for file %s, size %d", fileName, fileSize)
	checkArgs4AddArgs := &pb.CheckArgs4AddArgs{
		Path:     targetPath,
		FileName: fileName,
		Size:     fileSize,
	}
	reply, err := GlobalClientHandler.Check4Add(checkArgs4AddArgs)
	if err != nil {
		return nil, err
	}
	Logger.Debugf("file size: %v, chunk num: %v", fileSize, reply.ChunkNum)
	return &FileAddInfo{
		fileName:    fileName,
		fileNodeId:  reply.FileNodeId,
		targetPath:  targetPath,
		fileSize:    fileSize,
		chunkNum:    int(reply.ChunkNum),
		addTaskChan: make(chan *ChunkAddTask),
		resultChan:  make(chan *util.ChunkTaskResult, reply.ChunkNum),
	}, nil
}

// convertInput4Add converts input to some information of the file.
func convertInput4Add(src string, des string) (string, string, int64, error) {
	info, err := os.Stat(src)
	if err != nil {
		return "", "", 0, err
	}

	desPath := strings.Split(des, pathSplitString)
	if desPath[0] != "" {
		return "", "", 0, fmt.Errorf("Get the wrong path: %s\n", des)
	}
	desPathLength := len(desPath)
	var (
		fileName   string
		targetPath string
	)
	if desPath[desPathLength-1] == "" {
		srcPath := strings.Split(src, pathSplitString)
		srcPathLength := len(srcPath)
		fileName = srcPath[srcPathLength-1]
		targetPath = des
	} else {
		fileName = desPath[desPathLength-1]
		targetPath = strings.Join(desPath[:desPathLength-1], pathSplitString)
	}
	return fileName, targetPath, info.Size(), nil
}

// startConsumeTasks starts several consumers to consume ChunkAddTask.
func startConsumeTasks(info *FileAddInfo) *sync.WaitGroup {
	var wg sync.WaitGroup
	goroutineCount := maxGoroutineCount
	if maxGoroutineCount > info.chunkNum {
		goroutineCount = info.chunkNum
	}
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumeAddTasks(info)
		}()
	}
	return &wg
}

// sendTasks2Consumer gets DataNodes for each chunk of the file and give consumers
// ChunkAddTask.
func sendTasks2Consumer(info *FileAddInfo, src string) error {
	getDataNodes4AddArgs := &pb.GetDataNodes4AddArgs{
		FileNodeId: info.fileNodeId,
		ChunkNum:   int32(info.chunkNum),
	}
	reply, err := GlobalClientHandler.GetDataNodes4Add(getDataNodes4AddArgs)
	if err != nil {
		return err
	}
	for i := 0; i < info.chunkNum; i++ {
		file, err := os.OpenFile(src, os.O_RDWR, 0644)
		if err != nil {
			close(info.addTaskChan)
			close(info.resultChan)
			return err
		}
		info.addTaskChan <- &ChunkAddTask{
			file:         file,
			chunkIndex:   i,
			isLast:       i == info.chunkNum-1,
			dataNodeIds:  reply.DataNodeIds[i].Items,
			dataNodeAdds: reply.DataNodeAdds[i].Items,
		}
		Logger.Debugf("Chunk %v of %s add to addTaskChan", i, file.Name())
	}
	return nil
}

// callback2Master returns the result of an add operation to the master.
func callback2Master(info *FileAddInfo) error {
	infos := make([]*pb.ChunkInfo4Add, 0, info.chunkNum)
	failChunkIds := make([]string, 0, info.chunkNum)
	for result := range info.resultChan {
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
		FileNodeId:   info.fileNodeId,
		FilePath:     info.targetPath + pathSplitString + info.fileName,
		Infos:        infos,
		FailChunkIds: failChunkIds,
	}
	_, err := GlobalClientHandler.Callback4Add(unlockDic4AddArgs)
	return err
}

// ChunkAddTask represents a task of sending a chunk to several DataNodes.
type ChunkAddTask struct {
	file         *os.File
	chunkIndex   int
	isLast       bool
	dataNodeIds  []string
	dataNodeAdds []string
}

// consumeAddTasks gets ChunkAddTask from addTaskChan and establish a pipeline to send
// a Chunk to all target DataNode.
func consumeAddTasks(info *FileAddInfo) {
	for task := range info.addTaskChan {
		var (
			index         = task.chunkIndex
			chunkSize     int
			pieceNum      int
			lastPieceSize int
		)
		// Sometimes a DataNode may be allocated to store multiple Chunk of a file, and it may be the
		// first DataNode to receive data from client. If this DataNode crashes during transferring
		// data, the client will need to re-establish multiple pipelines. So client will shuffle the
		// order of DataNode in each pipeline.
		dataNodeIds, dataNodeAdds := randShuffle(task.dataNodeIds, task.dataNodeAdds)
		Logger.Debugf("Get datanodes, chunk id: %v, datanode ids: %v, datanode addresses: %v",
			index, task.dataNodeIds, task.dataNodeAdds)
		chunkId := info.fileNodeId + common.ChunkIdDelimiter + strconv.Itoa(index)
		currentResult := &util.ChunkTaskResult{
			ChunkId:          chunkId,
			FailDataNodes:    dataNodeIds,
			SuccessDataNodes: dataNodeIds[0:0],
		}

		chunkSize, pieceNum, lastPieceSize = calChunkInfo(info.fileSize, task.isLast)

		buffer, _ := unix.Mmap(int(task.file.Fd()), int64(index*common.ChunkSize), chunkSize,
			unix.PROT_WRITE, unix.MAP_SHARED)
		task.file.Close()
		// Calculate checksum for each piece and add them into the metadata of grpc stream.
		checkSums := CalCheckSum4Piece(buffer, pieceNum)

		currentResult = consumeSingleAddTask(chunkId, chunkSize, pieceNum, lastPieceSize, buffer, checkSums,
			dataNodeIds, dataNodeAdds, currentResult)
		_ = unix.Munmap(buffer)
		info.resultChan <- currentResult
		bar.Add(1)
	}
}

// getStream builds stream to transfer this chunk to primary chunkserver. Client
// will send some information to Chunkserver via grpc metadata. It includes:
// 1. The addresses of chunkservers that need to receive this chunk next.
// 2. The id of the current chunk.
// 3. The size of the current chunk.
// 2. The checksums of the current chunk.
func getStream(chunkId string, dataNodeAdds []string, chunkSize int, checkSums []string) (pb.PipLineService_TransferChunkClient, error) {
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
	for _, checkSum := range checkSums {
		newCtx = metadata.AppendToOutgoingContext(newCtx, common.CheckSumString, checkSum)
	}
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

// CalCheckSum4Piece calculates checksums for a chunk. Each piece will be calculated as a checksum.
func CalCheckSum4Piece(buffer []byte, pieceNum int) []string {
	checkSums := make([]string, pieceNum)
	for i := 0; i < pieceNum-1; i++ {
		checkSums[i] = util.CRC32String(buffer[common.MB*i : common.MB*(i+1)])
	}
	checkSums[pieceNum-1] = util.CRC32String(buffer[common.MB*(pieceNum-1):])
	return checkSums
}

// calChunkInfo calculates some information of a chunk.
func calChunkInfo(fileSize int64, isLast bool) (int, int, int) {
	var (
		chunkSize     int
		pieceNum      int
		lastPieceSize int
	)
	if isLast {
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
	return chunkSize, pieceNum, lastPieceSize
}

// consumeSingleAddTask consumes a ChunkAddTask which means it send a chunk to several DataNodes.
func consumeSingleAddTask(chunkId string, chunkSize int, pieceNum int, lastPieceSize int, buffer []byte, checkSums []string,
	dataNodeIds []string, dataNodeAdds []string, currentResult *util.ChunkTaskResult) *util.ChunkTaskResult {
	isSuccess := false
	for i := 0; i < len(dataNodeIds); i++ {
		Logger.Debugf("Chunk %s try %v datanode, id: %s, address: %s", chunkId, i, dataNodeIds[0], dataNodeAdds[0])
		stream, err := getStream(chunkId, dataNodeAdds, chunkSize, checkSums)
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
				break
			}

			if j == pieceNum-1 {
				reply, err := stream.CloseAndRecv()
				if err != nil || len(reply.FailAdds) == len(dataNodeIds) {
					Logger.Errorf("Fail to close stream, error detail: %s", err.Error())
					break
				}
				currentResult = util.ConvReply2SingleResult(reply, dataNodeIds, dataNodeAdds, common.AddSendType)
				isSuccess = true
			}
		}
		if isSuccess {
			break
		}
		// If not success, try next DataNode.
		dataNodeIds = append(dataNodeIds[1:], dataNodeIds[0])
		dataNodeAdds = append(dataNodeAdds[1:], dataNodeAdds[0])
	}
	return currentResult
}
