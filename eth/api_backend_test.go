package eth

import (
	//"bytes"
	"context"
	//"strconv"
	"testing"
	//"time"

	"github.com/domicon-labs/op-geth/common"
	//"github.com/domicon-labs/op-geth/crypto"
	"github.com/domicon-labs/op-geth/ethclient"
	"github.com/domicon-labs/op-geth/log"
	"github.com/domicon-labs/op-geth/rpc"
)

const (
	port       = "8545"
	privateKey = "2ffb28910709e79b8bf06d22c8289fd24f86853a9f9832cd0707acc0fe554610"
	dataStr    = ""
)

func TestUploadFileDataByParams(t *testing.T) {

	// client,err := ethclient.DialContext(context.TODO(),"http://127.0.0.1:" + port)
	// if err != nil {
	// 	println("DialContext-----err",err.Error())
	// }

	// priv, err := crypto.HexToECDSA(privateKey)
	// if err != nil {
	// 	println("HexToECDSA---err",err.Error())
	// }

	// index := 2
	// length := 1024
	// gasPrice := 200
	commit := []byte("commit")

	//dumper.Printf("commit-----%x",commit)
	println("commit-----", &commit)

	// s := strconv.Itoa(index)
	// data := bytes.Repeat([]byte(s), 1024)
	// sign := []byte("sign")
	// txHash := common.BytesToHash([]byte("2"))

	// for  {
	// time.Sleep(500 * time.Millisecond)
	// sender := crypto.PubkeyToAddress(priv.PublicKey)
	// submitter := common.HexToAddress("251b3740a02a1c5cf5ffcdf60d42ed2a8398ddc8")
	// err = client.UploadFileDataByParams(context.TODO(),sender,submitter,uint64(index),uint64(length),uint64(gasPrice),data,commit,sign,txHash)
	// if err != nil {
	// 	println("UploadFileDataByParams---err",err.Error())
	//}
	//index++
	// s := strconv.Itoa(index)
	// bytes.Repeat([]byte(s), 1024)
	// data = []byte(string(data))
	// txHash = common.BytesToHash([]byte(s))

	//	println("发送的交易哈希是txHash: ",txHash.String(),"data: ",s,"data length: ",len(data))
	// }

}

func TestGetFileDataByHash(t *testing.T) {

	client, err := ethclient.DialContext(context.TODO(), "http://127.0.0.1:"+port)
	if err != nil {
		println("DialContext-----err", err.Error())
	}

	fileData, err := client.GetFileDataByHash(context.TODO(), common.BytesToHash([]byte("2")))
	if err != nil {
		println("GetFileDataByHash---err", err.Error())
	}

	log.Info("test-----", "fileData", fileData)

}

func TestDiskFileData(t *testing.T) {
	client, err := ethclient.DialContext(context.TODO(), "http://127.0.0.1:"+port)
	if err != nil {
		println("DialContext-----err", err.Error())
	}

	flag, err := client.DiskSaveFileDataWithHash(context.TODO(), common.BytesToHash([]byte("2")))
	if err != nil {
		println("DiskSaveFileDataWithHash----err", err.Error())
	}
	log.Info("test-----", "flag", flag)
}

func TestGetFileDataByHashes(t *testing.T) {
	client, err := ethclient.DialContext(context.TODO(), "http://127.0.0.1:"+port)
	if err != nil {
		println("DialContext-----err", err.Error())
	}

	res, err := client.GetBatchFileDataByHashes(context.TODO(), rpc.TxHashes{TxHashes: []common.Hash{common.BytesToHash([]byte("2"))}})
	if err != nil {
		println("TestGetFileDataByHashes-----err", err.Error())
	}
	log.Info("test-----", "fds", res)
}

func TestChangeCurrentState(t *testing.T) {
	client, err := ethclient.DialContext(context.TODO(), "http://127.0.0.1:"+port)
	if err != nil {
		println("DialContext-----err", err.Error())
	}

	flag, err := client.ChangeCurrentState(context.TODO(), 1, rpc.BlockNumber(10))
	if err != nil {
		println("err----", err.Error())
	}
	println("flag-----", flag)
}

func TestDiskSaveFileDataWithHashes(t *testing.T) {
	client, err := ethclient.DialContext(context.TODO(), "http://127.0.0.1:"+port)
	if err != nil {
		println("DialContext-----err", err.Error())
	}

	res, err := client.DiskSaveFileDataWithHashes(context.TODO(), rpc.TxHashes{TxHashes: []common.Hash{common.BytesToHash([]byte("2"))}})
	if err != nil {
		println("DialContext-----err", err.Error())
	}
	log.Info("test-----", "fds", res)
}

func TestDiskSaveFileDatas(t *testing.T) {

	client, err := ethclient.DialContext(context.TODO(), "http://127.0.0.1:"+port)
	if err != nil {
		println("DialContext-----err", err.Error())
	}

	res, err := client.DiskSaveFileDataWithHashes(context.TODO(), rpc.TxHashes{TxHashes: []common.Hash{common.BytesToHash([]byte("2"))}, BlockHash: common.BytesToHash([]byte("3")), BlockNumber: rpc.BlockNumber(10)})
	if err != nil {
		println("DialContext----err", err.Error())
	}
	log.Info("test-------", "res", res)
}
