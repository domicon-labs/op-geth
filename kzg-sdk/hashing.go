package kzg_sdk

import (
	"encoding/binary"
	"github.com/domicon-labs/op-geth/common"
	"github.com/domicon-labs/op-geth/crypto"
)

// rlpHash encodes x and hashes the encoded bytes.
func rlpHash(x interface{}) (h common.Hash) {
	// sha := hasherPool.Get().(crypto.KeccakState)
	// defer hasherPool.Put(sha)
	// sha.Reset()
	// rlp.Encode(sha, x)
	crypto.Keccak256Hash()

	//sha.Read(h[:])
	return h
}

func uint64ToBigEndianHexBytes(value uint64) []byte {
	// 创建一个长度为 8 的字节切片
	byteData := make([]byte, 8)
	// 使用 binary.BigEndian.PutUint64 将 uint64 转换为大端字节序
	binary.BigEndian.PutUint64(byteData, value)
	return byteData
}
