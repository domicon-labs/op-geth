package types

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)


func FiledSender(signer Signer, fd *FileData) (common.Address, error) {


	

	//addr, err := signer.Sender(fd)
	// if err != nil {
	// 	return common.Address{}, err
	// }

	return common.Address{}, nil
}



type FileSigner interface {
	// Sender returns the sender address of the transaction.
	Sender(fd *FileData) (common.Address, error)

	// SignatureValues returns the raw R, S, V values corresponding to the
	// given signature.
	SignatureValues(tx *Transaction, sig []byte) (r, s, v *big.Int, err error)
	ChainID() *big.Int

	// Hash returns 'signature hash', i.e. the transaction hash that is signed by the
	// private key. This hash does not uniquely identify the transaction.
	Hash(tx *Transaction) common.Hash

	// Equal returns true if the given signer is the same as the receiver.
	Equal(Signer) bool
}


