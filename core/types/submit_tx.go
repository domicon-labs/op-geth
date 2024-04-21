package types

import (
	"bytes"
	"github.com/domicon-labs/op-geth/common"
	"github.com/domicon-labs/op-geth/rlp"
	"math/big"
)

const SubmitTxType = 0x7D

type SubmitTx struct {
	// SourceHash uniquely identifies the source of to submit
	SourceHash common.Hash
	// From is exposed through the types.Signer, not through TxData
	From common.Address
	// nil means contract creation
	To *common.Address `rlp:"nil"`
	// Value is transferred from L2 balance, executed after Mint (if any)
	Value *big.Int
	// gas limit
	Gas uint64
	// Field indicating if this transaction is exempt from the L2 gas limit.
	IsSystemTransaction bool
	// Normal Tx data
	Data []byte
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (tx *SubmitTx) copy() TxData {
	cpy := &SubmitTx{
		SourceHash:          tx.SourceHash,
		From:                tx.From,
		To:                  copyAddressPtr(tx.To),
		Value:               new(big.Int),
		Gas:                 tx.Gas,
		IsSystemTransaction: tx.IsSystemTransaction,
		Data:                common.CopyBytes(tx.Data),
	}
	if tx.Value != nil {
		cpy.Value.Set(tx.Value)
	}
	return cpy
}

// accessors for innerTx.
func (tx *SubmitTx) txType() byte           { return SubmitTxType }
func (tx *SubmitTx) chainID() *big.Int      { return common.Big0 }
func (tx *SubmitTx) accessList() AccessList { return nil }
func (tx *SubmitTx) data() []byte           { return tx.Data }
func (tx *SubmitTx) gas() uint64            { return tx.Gas }
func (tx *SubmitTx) gasFeeCap() *big.Int    { return new(big.Int) }
func (tx *SubmitTx) gasTipCap() *big.Int    { return new(big.Int) }
func (tx *SubmitTx) gasPrice() *big.Int     { return new(big.Int) }
func (tx *SubmitTx) value() *big.Int        { return tx.Value }
func (tx *SubmitTx) nonce() uint64          { return 0 }
func (tx *SubmitTx) to() *common.Address    { return tx.To }
func (tx *SubmitTx) isSystemTx() bool       { return tx.IsSystemTransaction }

func (tx *SubmitTx) effectiveGasPrice(dst *big.Int, baseFee *big.Int) *big.Int {
	return dst.Set(new(big.Int))
}

func (tx *SubmitTx) effectiveNonce() *uint64 { return nil }

func (tx *SubmitTx) rawSignatureValues() (v, r, s *big.Int) {
	return common.Big0, common.Big0, common.Big0
}

func (tx *SubmitTx) setSignatureValues(chainID, v, r, s *big.Int) {
	// this is a noop for deposit transactions
}

func (tx *SubmitTx) encode(b *bytes.Buffer) error {
	return rlp.Encode(b, tx)
}

func (tx *SubmitTx) decode(input []byte) error {
	return rlp.DecodeBytes(input, tx)
}
