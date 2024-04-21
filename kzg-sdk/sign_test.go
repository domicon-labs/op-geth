package kzg_sdk

import (
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/domicon-labs/op-geth/common"
	"github.com/domicon-labs/op-geth/crypto"
)

func TestEIP155FdSigning(t *testing.T) {
	key, _ := crypto.GenerateKey()
	senAddr := crypto.PubkeyToAddress(key.PublicKey)
	println("senAddr----", senAddr.Hex())
	//chain upflow is the max of uint64
	signer := NewEIP155FdSigner(big.NewInt(332111))
	index := 1
	length := 10
	gasPrice := 10
	commit := []byte("commit")
	//sign
	signHash, signData, err := SignFd(senAddr, senAddr, uint64(gasPrice), uint64(index), uint64(length), commit, signer, key)
	if err != nil {
		t.Errorf("err----- %x", err.Error())
	}

	// verify
	from, err := FdSender(signer, signData, signHash)
	if err != nil {
		println("err----", err.Error())
	}

	if from != senAddr {
		t.Errorf("exected from and address to be equal. Got %x want %x", from, senAddr)
	}

	from1, err := FdGetSender(signer, signData, senAddr, senAddr, uint64(gasPrice), uint64(index), uint64(length), commit)
	if err != nil {
		println("err----", err.Error())
	}
	if from1 != senAddr {
		t.Errorf("exected from and address to be equal. Got %x want %x", from, senAddr)
	}

}

func TestHomesteadFdSigner(t *testing.T) {
	key, _ := crypto.GenerateKey()
	subAddr := crypto.PubkeyToAddress(key.PublicKey)
	signer := HomesteadFdSigner{}

	key1, _ := crypto.GenerateKey()
	senAddr := crypto.PubkeyToAddress(key1.PublicKey)

	index := 1
	length := 10
	gasPrice := 10
	commit := []byte("commit")

	signHash, signData, err := SignFd(senAddr, subAddr, uint64(index), uint64(length), uint64(gasPrice), commit, signer, key)
	if err != nil {
		t.Errorf("err-----1 %x", err.Error())
	}

	from, err := FdSender(signer, signData, signHash)
	if err != nil {
		println("err----", err.Error())
	}

	if from != subAddr {
		t.Errorf("exected from and address to be equal. Got %x want %x", from, subAddr)
	}
}

func BenchmarkVeriftSign(b *testing.B) {
	b.Run("verify", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			signHash := common.HexToHash("0x194adeeafdb655b2da6c141abdcae1e908ed49c6986e45d72c3ca83658cd9721")
			signData, _ := hex.DecodeString("d02ee277524e10f0f59300bb60b7167ca741b37d85ad49b20ef13f782c35110d7d7898f13b27d649af86d28ca87df08ff8d8ca90d83863ea0a63f6a095145f291c")
			sender := common.HexToAddress("0x72B331Cde50eF0E183E007BB1050FF4b18aF59c1")
			signer := NewEIP155FdSigner(big.NewInt(11))
			from, err := FdSender(signer, signData, signHash)
			if err != nil {
				println("err----", err.Error())
			}

			if from != sender {
				println("error-----notcorrect", i)
			}
		}
	})

}
