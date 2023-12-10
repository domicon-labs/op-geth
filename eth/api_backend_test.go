package eth

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
)

const (
	port       = "8545"
	privateKey = "2ffb28910709e79b8bf06d22c8289fd24f86853a9f9832cd0707acc0fe554610"
	dataStr    = ""
)


func TestUploadFileDataByParams(t *testing.T){

	client,err := ethclient.DialContext(context.TODO(),"http://127.0.0.1:" + port)
	if err != nil {
		println("DialContext-----err",err.Error())
	}

	priv, err := crypto.HexToECDSA(privateKey)
	if err != nil {
		println("HexToECDSA---err",err.Error())
	}
	
	index := 0
	length := 10
	commit := []byte("commit")
	s := strconv.Itoa(index)
	data := bytes.Repeat([]byte(s), 1024)
	sign := []byte("sign")
	txHash := common.BytesToHash([]byte("1"))

	for  {
		time.Sleep(500 * time.Millisecond)
		sender := crypto.PubkeyToAddress(priv.PublicKey)
		submitter := common.HexToAddress("251b3740a02a1c5cf5ffcdf60d42ed2a8398ddc8")
		err = client.UploadFileDataByParams(context.TODO(),sender,submitter,uint64(index),uint64(length),data,commit,sign,txHash)
		if err != nil {
			println("UploadFileDataByParams---err",err.Error())
		}
		index++
		s := strconv.Itoa(index)
		bytes.Repeat([]byte(s), 1024)
		data = []byte(string(data))
		txHash = common.BytesToHash([]byte(s))

		println("发送的交易哈希是txHash: ",txHash.String(),"data: ",s,"data length: ",len(data))
	}
	
}


func TestGetFileDataByHash(t *testing.T){

	client,err := ethclient.DialContext(context.TODO(),"http://127.0.0.1:" + port)
	if err != nil {
		println("DialContext-----err",err.Error())
	}

	
	fileData,err := client.GetFileDataByHash(context.TODO(),common.BytesToHash([]byte("111111122222223333")))
	if err != nil {
		println("GetFileDataByHash---err",err.Error())
	}

	log.Info("test-----","fileData",fileData)
	
}