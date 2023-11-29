package eth

import (
	"context"
	"testing"

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
	sender := crypto.PubkeyToAddress(priv.PublicKey)
	submitter := common.HexToAddress("251b3740a02a1c5cf5ffcdf60d42ed2a8398ddc8")
	err = client.UploadFileDataByParams(context.TODO(),sender,submitter,1,10,[]byte("11112"),[]byte("commit"),[]byte("sign"),common.BytesToHash([]byte("11111112222222")))
	if err != nil {
		println("UploadFileDataByParams---err",err.Error())
	}
}



func TestGetFileDataByHash(t *testing.T){

	client,err := ethclient.DialContext(context.TODO(),"http://127.0.0.1:" + port)
	if err != nil {
		println("DialContext-----err",err.Error())
	}

	
	fileData,err := client.GetFileDataByHash(context.TODO(),common.BytesToHash([]byte("11111112222222")))
	if err != nil {
		println("GetFileDataByHash---err",err.Error())
	}

	log.Info("test-----","fileData",fileData)
	
}