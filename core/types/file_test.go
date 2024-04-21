package types

import (
	"fmt"
	"testing"

	"github.com/domicon-labs/op-geth/common"
	"github.com/domicon-labs/op-geth/crypto"
	"github.com/domicon-labs/op-geth/rlp"
)

const (
	privateKey = "2ffb28910709e79b8bf06d22c8289fd24f86853a9f9832cd0707acc0fe554610"
)

func TestNewFileData(t *testing.T) {

	priv, err := crypto.HexToECDSA(privateKey)
	if err != nil {
		println("HexToECDSA---err", err.Error())
	}
	sender := crypto.PubkeyToAddress(priv.PublicKey)
	submitter := common.HexToAddress("251b3740a02a1c5cf5ffcdf60d42ed2a8398ddc8")

	fileD := FileData{
		Sender:     sender,
		Submitter:  submitter,
		Index:      1,
		Length:     10,
		Commitment: []byte("commit"),
		Data:       []byte("1111233331111"),
		SignData:   []byte("sign"),
		TxHash:     common.BytesToHash([]byte("111111122222223333")),
	}

	data, err := rlp.EncodeToBytes(fileD)
	if err != nil {
		println("查看一下--2--压缩-err", err.Error())
	}

	println("data----", &data)

	var fd FileData

	err = rlp.DecodeBytes(data, &fd)
	if err != nil {
		println("解压---err", err.Error())
	}

	fmt.Println("Decoded FileData:", fd)
	println("解压信息----txHash:", fd.TxHash.String())
	println("信息---data:", string(fd.Data))

}
