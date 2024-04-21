/**
 * Copyright 2024.1.11
 * @Author: EchoWu
 * @Description: This file is part of the DOMICON library.
 */
package kzg_sdk

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"

	"github.com/domicon-labs/op-geth/common"
	"github.com/domicon-labs/op-geth/crypto"
)

var (
	ErrInvalidSig     = errors.New("invalid fileData v, r, s values")
	ErrInvalidChainId = errors.New("invalid chain id for signer")
)

// SignFd signs the fileData using the given signer and private key.
func SignFd(sender, submitter common.Address, gasPrice, index, length uint64, commitment []byte, signer FdSigner, prv *ecdsa.PrivateKey) (common.Hash, []byte, error) {
	h := signer.Hash(sender, submitter, gasPrice, index, length, commitment)
	sig, err := crypto.Sign(h[:], prv)
	if err != nil {
		return h, nil, err
	}
	if len(sig) == 0 {
		return h, nil, errors.New("sign is empty")
	}
	//  r, s, v, err := signer.SignatureValues(sig)
	//  if err != nil {
	// 	 return h,nil, err
	//  }
	v := []byte{sig[64] + 27}
	newSig := sig[:64]
	newSig = append(newSig, v...)
	return h, newSig, nil
}

// FdSender returns the address derived from the signature (V, R, S) using secp256k1
// elliptic curve and an error if it failed deriving or upon an incorrect
// signature.
func FdSender(signer FdSigner, sig []byte, signHash common.Hash) (common.Address, error) {
	addr, err := signer.Sender(sig, signHash)
	if err != nil {
		return common.Address{}, err
	}
	return addr, nil
}

func FdGetSender(signer FdSigner, sig []byte, sender, submmiter common.Address, gasPrice, index, length uint64, commitment []byte) (common.Address, error) {
	h := signer.Hash(sender, submmiter, gasPrice, index, length, commitment)
	addr, err := signer.Sender(sig, h)
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
	Sender(sig []byte, signHash common.Hash) (common.Address, error)

	// SignatureValues returns the raw R, S, V values corresponding to the
	// given signature.
	SignatureValues(sig []byte) (r, s, v *big.Int, err error)

	ChainID() *big.Int

	// Hash returns 'signature hash', i.e. the fileData hash that is signed by the
	// private key. This hash does not uniquely identify the fileData.
	Hash(sender, submitter common.Address, gasPrice, index, length uint64, commitment []byte) common.Hash

	// Equal returns true if the given signer is the same as the receiver.
	Equal(FdSigner) bool
}

//var big8 = big.NewInt(8)

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

func (s EIP155FdSigner) Sender(sig []byte, signHash common.Hash) (common.Address, error) {
	R, S, V := sliteSignature(sig)
	//  V = new(big.Int).Sub(V, s.chainIdMul)
	//  V.Sub(V, big8)
	return recoverPlain(signHash, R, S, V, true)
}

// SignatureValues returns signature values. This signature
// needs to be in the [R || S || V] format where V is 0 or 1.
func (s EIP155FdSigner) SignatureValues(sig []byte) (R, S, V *big.Int, err error) {
	R, S, V = decodeSignature(sig)
	//  if s.chainId.Sign() != 0 {
	// 	 V = big.NewInt(int64(sig[64] + 35))
	// 	 V.Add(V, s.chainIdMul)
	//  }
	return R, S, V, nil
}

// Hash returns the hash to be signed by the sender.
// It does not uniquely identify the transaction.
func (s EIP155FdSigner) Hash(sender, submitter common.Address, gasPrice, index, length uint64, commitment []byte) common.Hash {
	data := make([]byte, 0)
	data = append(data, uint64ToBigEndianHexBytes(s.chainId.Uint64())...)
	data = append(data, sender.Bytes()...)
	data = append(data, submitter.Bytes()...)
	data = append(data, uint64ToBigEndianHexBytes(gasPrice)...)
	data = append(data, uint64ToBigEndianHexBytes(index)...)
	data = append(data, uint64ToBigEndianHexBytes(length)...)
	data = append(data, commitment...)
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
func (hs HomesteadFdSigner) SignatureValues(sig []byte) (r, s, v *big.Int, err error) {
	return hs.FrontierFdSigner.SignatureValues(sig)
}

func (hs HomesteadFdSigner) Sender(sig []byte, signHash common.Hash) (common.Address, error) {
	r, s, v := decodeSignature(sig)
	v.Sub(v, new(big.Int).SetUint64(27))
	return recoverPlain(signHash, r, s, v, true)
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

func (fs FrontierFdSigner) Sender(sig []byte, signHash common.Hash) (common.Address, error) {
	r, s, v := sliteSignature(sig)
	v = v.Mul(v, new(big.Int).SetUint64(27))
	return recoverPlain(signHash, r, s, v, false)
}

// SignatureValues returns signature values. This signature
// needs to be in the [R || S || V] format where V is 0 or 1.
func (fs FrontierFdSigner) SignatureValues(sig []byte) (r, s, v *big.Int, err error) {
	r, s, v = decodeSignature(sig)
	return r, s, v, nil
}

// Hash returns the hash to be signed by the sender.
// It does not uniquely identify the transaction.
func (fs FrontierFdSigner) Hash(sender, submitter common.Address, gasPrice, index, length uint64, commitment []byte) common.Hash {
	return rlpHash([]interface{}{
		sender,
		submitter,
		gasPrice,
		index,
		length,
		commitment,
	})
}

func decodeSignature(sig []byte) (r, s, v *big.Int) {
	if len(sig) != crypto.SignatureLength {
		panic(fmt.Sprintf("wrong size for signature: got %d, want %d", len(sig), crypto.SignatureLength))
	}
	r = new(big.Int).SetBytes(sig[:32])
	s = new(big.Int).SetBytes(sig[32:64])
	v = new(big.Int).SetBytes([]byte{sig[64] + 27})
	return r, s, v
}

func sliteSignature(sig []byte) (r, s, v *big.Int) {
	r = new(big.Int).SetBytes(sig[:32])
	s = new(big.Int).SetBytes(sig[32:64])
	v = new(big.Int).SetBytes(sig[64:])
	return r, s, v
}

func recoverPlain(sighash common.Hash, R, S, Vb *big.Int, homestead bool) (common.Address, error) {
	if Vb.BitLen() > 8 {
		return common.Address{}, ErrInvalidSig
	}
	V := byte(Vb.Uint64() - 27)
	if !crypto.ValidateSignatureValues(V, R, S, homestead) {
		return common.Address{}, ErrInvalidSig
	}
	// encode the signature in uncompressed format
	r, s := R.Bytes(), S.Bytes()
	sig := make([]byte, crypto.SignatureLength)
	copy(sig[32-len(r):32], r)
	copy(sig[64-len(s):64], s)
	sig[64] = V
	// recover the public key from the signature
	pub, err := crypto.Ecrecover(sighash[:], sig)
	if err != nil {
		return common.Address{}, err
	}
	if len(pub) == 0 || pub[0] != 4 {
		return common.Address{}, errors.New("invalid public key")
	}
	var addr common.Address
	copy(addr[:], crypto.Keccak256(pub[1:])[12:])
	return addr, nil
}

// deriveChainId derives the chain id from the given v parameter
func deriveChainId(v *big.Int) *big.Int {
	if v.BitLen() <= 64 {
		v := v.Uint64()
		if v == 27 || v == 28 {
			return new(big.Int)
		}
		return new(big.Int).SetUint64((v - 35) / 2)
	}
	v = new(big.Int).Sub(v, big.NewInt(35))
	return v.Div(v, big.NewInt(2))
}
