package internal

import (
	"github.com/sirupsen/logrus"
	"io"
	"sync"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

func DoTransferFile(stream pb.PipLineService_TransferChunkServer, chunkIndex int32) error {
	var (
		wg        = &sync.WaitGroup{}
		pieceChan = make(chan *pb.PieceOfChunk)
		errChan   = make(chan error)
	)
	go func() {
		wg.Add(1)
		defer wg.Done()
		storeFile(pieceChan, errChan, chunkIndex)
	}()
	// Receive pieces of chunk until there are no more pieces
	for {
		pieceOfChunk, err := stream.Recv()
		if err == io.EOF {
			close(pieceChan)
			err = stream.SendAndClose(&pb.TransferChunkReply{})
			if err != nil {
				logrus.Errorf("fail to close receive stream, error detail: %s", err.Error())
				return err
			}
			// Main thread will wait until goroutine success to store the block.
			wg.Wait()
			if len(errChan) != 0 {
				err = <-errChan
				return err
			}
			logrus.Infof("%d write done!\n", chunkIndex)
			return nil
		}
		pieceChan <- pieceOfChunk
	}
}

func storeFile(pieceChan chan *pb.PieceOfChunk, errChan chan error, index int32) {
	defer func() {
		close(errChan)
		file.Close()
	}()
	// Goroutine will be blocked until main thread receive pieces of chunk and put them into pieceChan
	file.Seek(int64(common.ChunkSize*index), 0)
	for piece := range pieceChan {
		if _, err := file.Write(piece.Piece); err != nil {
			logrus.Errorf("fail to write a piece to chunk file, error detail: %s\n", err.Error())
			errChan <- err
			break
		}
	}
}
