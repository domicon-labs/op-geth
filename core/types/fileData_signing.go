/**
 * Copyright 2024.1.11
 * @Author: EchoWu
 * @Description: This file is part of the DOMICON library.
 */
package types

import (
	"crypto/ecdsa"
	"encoding/binary"
	"math/big"

	"github.com/domicon-labs/op-geth/common"
	"github.com/domicon-labs/op-geth/crypto"
	"github.com/domicon-labs/op-geth/params"
)

//var ErrInvalidChainId = errors.New("invalid chain id for signer")

// sigFdCache is used to cache the derived sender and contains
// the signer used to derive it.
// type sigFdCache struct {
// 	signer FdSigner
// 	from   common.Address
// }

// MakeFdSigner returns a Signer based on the given chain config and block number.
func MakeFdSigner(config *params.ChainConfig, blockNumber *big.Int, blockTime uint64) FdSigner {
	var signer FdSigner
	switch {
	// case config.IsCancun(blockNumber, blockTime):
	// 	signer = NewCancunFdSigner(config.ChainID)
	// case config.IsLondon(blockNumber):
	// 	signer = NewLondonFdSigner(config.ChainID)
	// case config.IsBerlin(blockNumber):
	// 	signer = NewEIP2930FdSigner(config.ChainID)
	case config.IsEIP155(blockNumber):
		signer = NewEIP155FdSigner(config.ChainID)
	case config.IsHomestead(blockNumber):
		signer = HomesteadFdSigner{}
	default:
		signer = FrontierFdSigner{}
	}
	return signer
}

// LatestFdSigner returns the 'most permissive' Signer available for the given chain
// configuration. Specifically, this enables support of all types of fileDatas
// when their respective forks are scheduled to occur at any block number (or time)
// in the chain config.
//
// Use this in fileData-handling code where the current block number is unknown. If you
// have the current block number available, use MakeSigner instead.
func LatestFdSigner(config *params.ChainConfig) FdSigner {
	if config.ChainID != nil {
		// if config.CancunTime != nil {
		// 	return NewCancunFdSigner(config.ChainID)
		// }
		// if config.LondonBlock != nil {
		// 	return NewLondonFdSigner(config.ChainID)
		// }
		// if config.BerlinBlock != nil {
		// 	return NewEIP2930FdSigner(config.ChainID)
		// }
		if config.EIP155Block != nil {
			return NewEIP155FdSigner(config.ChainID)
		}
	}
	return HomesteadFdSigner{}
}

// LatestFdSignerForChainID returns the 'most permissive' Signer available. Specifically,
// this enables support for EIP-155 replay protection and all implemented EIP-2718
// fileData types if chainID is non-nil.
//
// Use this in fileData-handling code where the current block number and fork
// configuration are unknown. If you have a ChainConfig, use LatestSigner instead.
// If you have a ChainConfig and know the current block number, use MakeSigner instead.
func LatestFdSignerForChainID(chainID *big.Int) FdSigner {
	if chainID == nil {
		return HomesteadFdSigner{}
	}
	return NewEIP155FdSigner(chainID)
}

// SignFd signs the fileData using the given signer and private key.
func SignFd(fd *FileData, s FdSigner, prv *ecdsa.PrivateKey) (*FileData, error) {
	h := s.Hash(fd)
	sig, err := crypto.Sign(h[:], prv)
	if err != nil {
		return nil, err
	}
	return fd.WithSignature(s, sig)
}

// FdSender returns the address derived from the signature (V, R, S) using secp256k1
// elliptic curve and an error if it failed deriving or upon an incorrect
// signature.
func FdSender(signer FdSigner, fd *FileData) (common.Address, error) {
	addr, err := signer.Sender(fd)
	if err != nil {
		return common.Address{}, err
	}
	return addr, nil
}

// FdSigner encapsulates fileData signature handling. The name of this type is slightly
// misleading because Signers don't actually sign, they're just for validating and
// processing of signatures.
//
// Note that this interface is not a stable API and may change at any time to accommodate
// new protocol rules.
type FdSigner interface {
	// Sender returns the sender address of the fileData.
	Sender(fd *FileData) (common.Address, error)

	// SignatureValues returns the raw R, S, V values corresponding to the
	// given signature.
	SignatureValues(fd *FileData, sig []byte) (r, s, v *big.Int, err error)

	ChainID() *big.Int

	// Hash returns 'signature hash', i.e. the fileData hash that is signed by the
	// private key. This hash does not uniquely identify the fileData.
	Hash(fd *FileData) common.Hash

	// Equal returns true if the given signer is the same as the receiver.
	Equal(FdSigner) bool
}

// EIP155Signer implements Signer using the EIP-155 rules. This accepts transactions which
// are replay-protected as well as unprotected homestead transactions.
type EIP155FdSigner struct {
	chainId, chainIdMul *big.Int
}

func NewEIP155FdSigner(chainId *big.Int) EIP155FdSigner {
	if chainId == nil {
		chainId = new(big.Int)
	}
	return EIP155FdSigner{
		chainId:    chainId,
		chainIdMul: new(big.Int).Mul(chainId, big.NewInt(2)),
	}
}

func (s EIP155FdSigner) ChainID() *big.Int {
	return s.chainId
}

func (s EIP155FdSigner) Equal(s2 FdSigner) bool {
	eip155, ok := s2.(EIP155FdSigner)
	return ok && eip155.chainId.Cmp(s.chainId) == 0
}

func (s EIP155FdSigner) Sender(fd *FileData) (common.Address, error) {
	R, S, V := sliteSignature(fd.SignData)
	// V = new(big.Int).Sub(V, s.chainIdMul)
	// V.Sub(V, big8)
	return recoverPlain(s.Hash(fd), R, S, V, true)
}

// SignatureValues returns signature values. This signature
// needs to be in the [R || S || V] format where V is 0 or 1.
func (s EIP155FdSigner) SignatureValues(fd *FileData, sig []byte) (R, S, V *big.Int, err error) {
	R, S, V = decodeSignature(sig)
	// if s.chainId.Sign() != 0 {
	// 	V = big.NewInt(int64(sig[64] + 35))
	// 	V.Add(V, s.chainIdMul)
	// }
	return R, S, V, nil
}

// Hash returns the hash to be signed by the sender.
// It does not uniquely identify the transaction.
func (s EIP155FdSigner) Hash(fd *FileData) common.Hash {
	data := make([]byte, 0)
	data = append(data, uint64ToBigEndianHexBytes(s.chainId.Uint64())...)
	data = append(data, fd.Sender.Bytes()...)
	data = append(data, fd.Submitter.Bytes()...)
	data = append(data, uint64ToBigEndianHexBytes(fd.GasPrice)...)
	data = append(data, uint64ToBigEndianHexBytes(fd.Index)...)
	data = append(data, uint64ToBigEndianHexBytes(fd.Length)...)
	data = append(data, fd.Commitment...)
	return crypto.Keccak256Hash(data)
}

// HomesteadFdSigner implements Signer interface using the
// homestead rules.
type HomesteadFdSigner struct{ FrontierFdSigner }

func (s HomesteadFdSigner) ChainID() *big.Int {
	return nil
}

func (s HomesteadFdSigner) Equal(s2 FdSigner) bool {
	_, ok := s2.(HomesteadFdSigner)
	return ok
}

// SignatureValues returns signature values. This signature
// needs to be in the [R || S || V] format where V is 0 or 1.
func (hs HomesteadFdSigner) SignatureValues(fd *FileData, sig []byte) (r, s, v *big.Int, err error) {
	return hs.FrontierFdSigner.SignatureValues(fd, sig)
}

func (hs HomesteadFdSigner) Sender(fd *FileData) (common.Address, error) {
	r, s, v := fd.RawSignatureValues()
	v.Sub(v, new(big.Int).SetUint64(27))
	return recoverPlain(hs.Hash(fd), r, s, v, true)
}

// FrontierFdSigner implements Signer interface using the
// frontier rules.
type FrontierFdSigner struct{}

func (s FrontierFdSigner) ChainID() *big.Int {
	return nil
}

func (s FrontierFdSigner) Equal(s2 FdSigner) bool {
	_, ok := s2.(FrontierFdSigner)
	return ok
}

func (fs FrontierFdSigner) Sender(fd *FileData) (common.Address, error) {
	r, s, v := sliteSignature(fd.SignData)
	v = v.Mul(v, new(big.Int).SetUint64(27))
	return recoverPlain(fs.Hash(fd), r, s, v, false)
}

// SignatureValues returns signature values. This signature
// needs to be in the [R || S || V] format where V is 0 or 1.
func (fs FrontierFdSigner) SignatureValues(fd *FileData, sig []byte) (r, s, v *big.Int, err error) {
	r, s, v = decodeSignature(sig)
	return r, s, v, nil
}

// Hash returns the hash to be signed by the sender.
// It does not uniquely identify the transaction.
func (fs FrontierFdSigner) Hash(fd *FileData) common.Hash {
	data := make([]byte, 0)
	data = append(data, fd.Sender.Bytes()...)
	data = append(data, fd.Submitter.Bytes()...)
	data = append(data, uint64ToBigEndianHexBytes(fd.GasPrice)...)
	data = append(data, uint64ToBigEndianHexBytes(fd.Index)...)
	data = append(data, uint64ToBigEndianHexBytes(fd.Length)...)
	data = append(data, fd.Commitment...)
	return crypto.Keccak256Hash(data)
}

func sliteSignature(sig []byte) (r, s, v *big.Int) {
	r = new(big.Int).SetBytes(sig[:32])
	s = new(big.Int).SetBytes(sig[32:64])
	v = new(big.Int).SetBytes(sig[64:])
	return r, s, v
}

func uint64ToBigEndianHexBytes(value uint64) []byte {
	// 创建一个长度为 8 的字节切片
	byteData := make([]byte, 8)
	// 使用 binary.BigEndian.PutUint64 将 uint64 转换为大端字节序
	binary.BigEndian.PutUint64(byteData, value)
	return byteData
}
