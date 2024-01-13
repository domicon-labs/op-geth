package types

import (
	"bytes"
	"math/big"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)


func TestEIP155FdSigning(t *testing.T) {

	privateKey := "3180b6cc1ef8d68c00dc30c83b9f00321a60dbeeac202e7671312dc0cd9707b9"
	key, err := crypto.HexToECDSA(privateKey)
	if err != nil {
		println("err----",err.Error())
	}
	senAddr := crypto.PubkeyToAddress(key.PublicKey)

	println("senderAddr----",senAddr.Hex())

	signer := NewEIP155FdSigner(big.NewInt(18))

	key1, _ := crypto.GenerateKey()
	subAddr := crypto.PubkeyToAddress(key1.PublicKey)

	index := 1
	length := 10
	gasPrice := 10
	commit := []byte("commit")
	str := strconv.Itoa(index)
	data := bytes.Repeat([]byte(str), 1024)
	txHash := common.BytesToHash([]byte("2"))

	fd,err := SignFd(NewFileData(senAddr,subAddr,uint64(index),uint64(length),uint64(gasPrice),commit,data,[]byte{},txHash),signer, key)
	if err != nil {
		t.Errorf("err-----1 %x",err.Error())
	}


	from, err := FdSender(signer,fd)
	if err != nil {
		println("err----",err.Error())
	}

	if from != senAddr {
		t.Errorf("exected from and address to be equal. Got %x want %x", from, subAddr)
	}
}



func TestHomesteadFdSigner(t *testing.T) {
	key, _ := crypto.GenerateKey()
	subAddr := crypto.PubkeyToAddress(key.PublicKey)
	signer := HomesteadFdSigner{}
	//NewEIP155FdSigner(big.NewInt(18))

	key1, _ := crypto.GenerateKey()
	senAddr := crypto.PubkeyToAddress(key1.PublicKey)

	index := 1
	length := 10
	gasPrice := 10
	commit := []byte("commit")
	str := strconv.Itoa(index)
	data := bytes.Repeat([]byte(str), 1024)
	txHash := common.BytesToHash([]byte("2"))

	fd,err := SignFd(NewFileData(senAddr,subAddr,uint64(index),uint64(length),uint64(gasPrice),commit,data,[]byte{},txHash),signer, key)
	if err != nil {
		t.Errorf("err-----1 %x",err.Error())
	}

	from, err := FdSender(signer,fd)
	if err != nil {
		println("err----",err.Error())
	}

	if from != subAddr {
		t.Errorf("exected from and address to be equal. Got %x want %x", from, subAddr)
	}
}
