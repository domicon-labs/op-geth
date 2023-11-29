package types

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// FileData struct
// sender       address
// submitter    address
// index        uint64
// commitment   证据
// data         file
// sign         签名
type FileData struct {
	sender     common.Address  //文件发送者
	submitter  common.Address	//文件上传提交者
	index      uint64			//文件发送者类nonce 相同的index认为是重复交易
	length     uint64			//长度
	commitment []byte			//对应data的commitment
	data       []byte			//上传的的文件
	signData   []byte			//签名 sender sign [i,length,commitment,sender,submitter]
	txHash     common.Hash		//
}

func NewFileData(sender, submitter common.Address, index,length uint64, commitment, data, sign []byte,txHash common.Hash) *FileData {
	return &FileData{
		sender:     sender,
		submitter:  submitter,
		length:	    length,
		index:      index,
		commitment: commitment,
		data:       data,
		signData:   sign,
		txHash: 	txHash,
	}
}

func (f *FileData) Sender() common.Address {
	return f.sender
}

func (f *FileData) Submitter() common.Address {
	return f.submitter
}

func (f *FileData) Index() uint64 {
	return f.index
}

func (f *FileData) DataLength() uint64{
	return f.length
}

func (f *FileData) Commitment() []byte {
	return f.commitment
}

func (f *FileData) UploadData() []byte {
	return f.data
}

func (f *FileData) TxHash() common.Hash{
	return f.txHash
}

func (f *FileData) Sign() []byte{
	return f.signData
}


func (f *FileData) Encode() ([]byte, error) {
	data, err := rlp.EncodeToBytes(f)
	return data, err
}

func (f *FileData) Decode(data []byte) error {
	return rlp.DecodeBytes(data, f)
}

func (f *FileData) MarshalJSON() ([]byte,error) {
	log.Info("FileData----","MarshalJSON iscalling")
	return rlp.EncodeToBytes(f)
}

func (f *FileData) UnmarshalJSON(data []byte) error {
	return rlp.DecodeBytes(data,f)
} 

type FileDatas []*FileData

func (f FileDatas) Len() int { return len(f) }