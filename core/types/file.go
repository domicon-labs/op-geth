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
	Sender     common.Address  //文件发送者
	Submitter  common.Address	//文件上传提交者
	Index      uint64			//文件发送者类nonce 相同的index认为是重复交易
	Length     uint64			//长度
	Commitment []byte			//对应data的commitment
	Data       []byte			//上传的的文件
	SignData   []byte			//签名 sender sign [i,length,commitment,sender,submitter]
	TxHash     common.Hash		//
}

func NewFileData(sender, submitter common.Address, index,length uint64, commitment, data, sign []byte,txHash common.Hash) *FileData {
	return &FileData{
		Sender:     sender,
		Submitter:  submitter,
		Index:      index,
		Length:	    length,
		Commitment: commitment,
		Data:       data,
		SignData:   sign,
		TxHash: 	txHash,
	}
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