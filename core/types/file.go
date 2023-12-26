package types

import (
	"github.com/ethereum/go-ethereum/common"
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
	Sender     common.Address  	`json:"Sender"` //文件发送者
	Submitter  common.Address		`json:"Submitter"`//文件上传提交者
	Index      uint64						`json:"Index"`//文件发送者类nonce 相同的index认为是重复交易
	Length     uint64						`json:"Length"`//长度
	Commitment []byte						`json:"Commitment"`//对应data的commitment
	Data       []byte						`json:"Data"`//上传的的文件
	SignData   []byte						`json:"SignData"`//签名 sender sign [i,length,commitment,sender,submitter]
	TxHash     common.Hash			`json:"TxHash"`//
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

func (f *FileData) Size() uint64 {
	data,_ := rlp.EncodeToBytes(f)
	return uint64(len(data))
}

type FileDatas []*FileData

func (f FileDatas) Len() int { return len(f) }