// Copyright 2021 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/domicon-labs/op-geth/common"
	"github.com/domicon-labs/op-geth/core"
	"github.com/domicon-labs/op-geth/core/txpool/filedatapool"
	"github.com/domicon-labs/op-geth/core/types"
	"github.com/domicon-labs/op-geth/log"
	"github.com/domicon-labs/op-geth/rlp"
	"github.com/domicon-labs/op-geth/trie"
)

func handleGetBlockHeaders(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the complex header query
	var query GetBlockHeadersPacket
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	response := ServiceGetBlockHeadersQuery(backend.Chain(), query.GetBlockHeadersRequest, peer)
	return peer.ReplyBlockHeadersRLP(query.RequestId, response)
}

// ServiceGetBlockHeadersQuery assembles the response to a header query. It is
// exposed to allow external packages to test protocol behavior.
func ServiceGetBlockHeadersQuery(chain *core.BlockChain, query *GetBlockHeadersRequest, peer *Peer) []rlp.RawValue {
	if query.Skip == 0 {
		// The fast path: when the request is for a contiguous segment of headers.
		return serviceContiguousBlockHeaderQuery(chain, query)
	} else {
		return serviceNonContiguousBlockHeaderQuery(chain, query, peer)
	}
}

func serviceNonContiguousBlockHeaderQuery(chain *core.BlockChain, query *GetBlockHeadersRequest, peer *Peer) []rlp.RawValue {
	hashMode := query.Origin.Hash != (common.Hash{})
	first := true
	maxNonCanonical := uint64(100)

	// Gather headers until the fetch or network limits is reached
	var (
		bytes   common.StorageSize
		headers []rlp.RawValue
		unknown bool
		lookups int
	)
	for !unknown && len(headers) < int(query.Amount) && bytes < softResponseLimit &&
		len(headers) < maxHeadersServe && lookups < 2*maxHeadersServe {
		lookups++
		// Retrieve the next header satisfying the query
		var origin *types.Header
		if hashMode {
			if first {
				first = false
				origin = chain.GetHeaderByHash(query.Origin.Hash)
				if origin != nil {
					query.Origin.Number = origin.Number.Uint64()
				}
			} else {
				origin = chain.GetHeader(query.Origin.Hash, query.Origin.Number)
			}
		} else {
			origin = chain.GetHeaderByNumber(query.Origin.Number)
		}
		if origin == nil {
			break
		}
		if rlpData, err := rlp.EncodeToBytes(origin); err != nil {
			log.Crit("Unable to encode our own headers", "err", err)
		} else {
			headers = append(headers, rlp.RawValue(rlpData))
			bytes += common.StorageSize(len(rlpData))
		}
		// Advance to the next header of the query
		switch {
		case hashMode && query.Reverse:
			// Hash based traversal towards the genesis block
			ancestor := query.Skip + 1
			if ancestor == 0 {
				unknown = true
			} else {
				query.Origin.Hash, query.Origin.Number = chain.GetAncestor(query.Origin.Hash, query.Origin.Number, ancestor, &maxNonCanonical)
				unknown = (query.Origin.Hash == common.Hash{})
			}
		case hashMode && !query.Reverse:
			// Hash based traversal towards the leaf block
			var (
				current = origin.Number.Uint64()
				next    = current + query.Skip + 1
			)
			if next <= current {
				infos, _ := json.MarshalIndent(peer.Peer.Info(), "", "  ")
				peer.Log().Warn("GetBlockHeaders skip overflow attack", "current", current, "skip", query.Skip, "next", next, "attacker", infos)
				unknown = true
			} else {
				if header := chain.GetHeaderByNumber(next); header != nil {
					nextHash := header.Hash()
					expOldHash, _ := chain.GetAncestor(nextHash, next, query.Skip+1, &maxNonCanonical)
					if expOldHash == query.Origin.Hash {
						query.Origin.Hash, query.Origin.Number = nextHash, next
					} else {
						unknown = true
					}
				} else {
					unknown = true
				}
			}
		case query.Reverse:
			// Number based traversal towards the genesis block
			if query.Origin.Number >= query.Skip+1 {
				query.Origin.Number -= query.Skip + 1
			} else {
				unknown = true
			}

		case !query.Reverse:
			// Number based traversal towards the leaf block
			query.Origin.Number += query.Skip + 1
		}
	}
	return headers
}

func serviceContiguousBlockHeaderQuery(chain *core.BlockChain, query *GetBlockHeadersRequest) []rlp.RawValue {
	count := query.Amount
	if count > maxHeadersServe {
		count = maxHeadersServe
	}
	if query.Origin.Hash == (common.Hash{}) {
		// Number mode, just return the canon chain segment. The backend
		// delivers in [N, N-1, N-2..] descending order, so we need to
		// accommodate for that.
		from := query.Origin.Number
		if !query.Reverse {
			from = from + count - 1
		}
		headers := chain.GetHeadersFrom(from, count)
		if !query.Reverse {
			for i, j := 0, len(headers)-1; i < j; i, j = i+1, j-1 {
				headers[i], headers[j] = headers[j], headers[i]
			}
		}
		return headers
	}
	// Hash mode.
	var (
		headers []rlp.RawValue
		hash    = query.Origin.Hash
		header  = chain.GetHeaderByHash(hash)
	)
	if header != nil {
		rlpData, _ := rlp.EncodeToBytes(header)
		headers = append(headers, rlpData)
	} else {
		// We don't even have the origin header
		return headers
	}
	num := header.Number.Uint64()
	if !query.Reverse {
		// Theoretically, we are tasked to deliver header by hash H, and onwards.
		// However, if H is not canon, we will be unable to deliver any descendants of
		// H.
		if canonHash := chain.GetCanonicalHash(num); canonHash != hash {
			// Not canon, we can't deliver descendants
			return headers
		}
		descendants := chain.GetHeadersFrom(num+count-1, count-1)
		for i, j := 0, len(descendants)-1; i < j; i, j = i+1, j-1 {
			descendants[i], descendants[j] = descendants[j], descendants[i]
		}
		headers = append(headers, descendants...)
		return headers
	}
	{ // Last mode: deliver ancestors of H
		for i := uint64(1); header != nil && i < count; i++ {
			header = chain.GetHeaderByHash(header.ParentHash)
			if header == nil {
				break
			}
			rlpData, _ := rlp.EncodeToBytes(header)
			headers = append(headers, rlpData)
		}
		return headers
	}
}

func handleGetBlockBodies(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the block body retrieval message
	var query GetBlockBodiesPacket
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	response := ServiceGetBlockBodiesQuery(backend.Chain(), query.GetBlockBodiesRequest)
	return peer.ReplyBlockBodiesRLP(query.RequestId, response)
}

// ServiceGetBlockBodiesQuery assembles the response to a body query. It is
// exposed to allow external packages to test protocol behavior.
func ServiceGetBlockBodiesQuery(chain *core.BlockChain, query GetBlockBodiesRequest) []rlp.RawValue {
	// Gather blocks until the fetch or network limits is reached
	var (
		bytes  int
		bodies []rlp.RawValue
	)
	for lookups, hash := range query {
		if bytes >= softResponseLimit || len(bodies) >= maxBodiesServe ||
			lookups >= 2*maxBodiesServe {
			break
		}
		if data := chain.GetBodyRLP(hash); len(data) != 0 {
			bodies = append(bodies, data)
			bytes += len(data)
		}
	}
	return bodies
}

func handleGetReceipts(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the block receipts retrieval message
	var query GetReceiptsPacket
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	response := ServiceGetReceiptsQuery(backend.Chain(), query.GetReceiptsRequest)
	return peer.ReplyReceiptsRLP(query.RequestId, response)
}

// ServiceGetReceiptsQuery assembles the response to a receipt query. It is
// exposed to allow external packages to test protocol behavior.
func ServiceGetReceiptsQuery(chain *core.BlockChain, query GetReceiptsRequest) []rlp.RawValue {
	// Gather state data until the fetch or network limits is reached
	var (
		bytes    int
		receipts []rlp.RawValue
	)
	for lookups, hash := range query {
		if bytes >= softResponseLimit || len(receipts) >= maxReceiptsServe ||
			lookups >= 2*maxReceiptsServe {
			break
		}
		// Retrieve the requested block's receipts
		results := chain.GetReceiptsByHash(hash)
		if results == nil {
			if header := chain.GetHeaderByHash(hash); header == nil || header.ReceiptHash != types.EmptyRootHash {
				continue
			}
		}
		// If known, encode and queue for response packet
		if encoded, err := rlp.EncodeToBytes(results); err != nil {
			log.Error("Failed to encode receipt", "err", err)
		} else {
			receipts = append(receipts, encoded)
			bytes += len(encoded)
		}
	}
	return receipts
}

func handleNewBlockhashes(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of new block announcements just arrived
	ann := new(NewBlockHashesPacket)
	if err := msg.Decode(ann); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	// Mark the hashes as present at the remote node
	for _, block := range *ann {
		peer.markBlock(block.Hash)
	}
	// Deliver them all to the backend for queuing
	return backend.Handle(peer, ann)
}

func handleNewBlock(backend Backend, msg Decoder, peer *Peer) error {
	// Retrieve and decode the propagated block
	ann := new(NewBlockPacket)
	if err := msg.Decode(ann); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	if err := ann.sanityCheck(); err != nil {
		return err
	}
	if hash := types.CalcUncleHash(ann.Block.Uncles()); hash != ann.Block.UncleHash() {
		log.Warn("Propagated block has invalid uncles", "have", hash, "exp", ann.Block.UncleHash())
		return nil // TODO(karalabe): return error eventually, but wait a few releases
	}
	if hash := types.DeriveSha(ann.Block.Transactions(), trie.NewStackTrie(nil)); hash != ann.Block.TxHash() {
		log.Warn("Propagated block has invalid body", "have", hash, "exp", ann.Block.TxHash())
		return nil // TODO(karalabe): return error eventually, but wait a few releases
	}
	ann.Block.ReceivedAt = msg.Time()
	ann.Block.ReceivedFrom = peer

	// Mark the peer as owning the block
	peer.markBlock(ann.Block.Hash())

	return backend.Handle(peer, ann)
}

func handleBlockHeaders(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of headers arrived to one of our previous requests
	res := new(BlockHeadersPacket)
	if err := msg.Decode(res); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	metadata := func() interface{} {
		hashes := make([]common.Hash, len(res.BlockHeadersRequest))
		for i, header := range res.BlockHeadersRequest {
			hashes[i] = header.Hash()
		}
		return hashes
	}
	return peer.dispatchResponse(&Response{
		id:   res.RequestId,
		code: BlockHeadersMsg,
		Res:  &res.BlockHeadersRequest,
	}, metadata)
}

func handleBlockBodies(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of block bodies arrived to one of our previous requests
	res := new(BlockBodiesPacket)
	if err := msg.Decode(res); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	metadata := func() interface{} {
		var (
			txsHashes        = make([]common.Hash, len(res.BlockBodiesResponse))
			uncleHashes      = make([]common.Hash, len(res.BlockBodiesResponse))
			withdrawalHashes = make([]common.Hash, len(res.BlockBodiesResponse))
		)
		hasher := trie.NewStackTrie(nil)
		for i, body := range res.BlockBodiesResponse {
			txsHashes[i] = types.DeriveSha(types.Transactions(body.Transactions), hasher)
			uncleHashes[i] = types.CalcUncleHash(body.Uncles)
			if body.Withdrawals != nil {
				withdrawalHashes[i] = types.DeriveSha(types.Withdrawals(body.Withdrawals), hasher)
			}
		}
		return [][]common.Hash{txsHashes, uncleHashes, withdrawalHashes}
	}
	return peer.dispatchResponse(&Response{
		id:   res.RequestId,
		code: BlockBodiesMsg,
		Res:  &res.BlockBodiesResponse,
	}, metadata)
}

func handleReceipts(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of receipts arrived to one of our previous requests
	res := new(ReceiptsPacket)
	if err := msg.Decode(res); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	metadata := func() interface{} {
		hasher := trie.NewStackTrie(nil)
		hashes := make([]common.Hash, len(res.ReceiptsResponse))
		for i, receipt := range res.ReceiptsResponse {
			hashes[i] = types.DeriveSha(types.Receipts(receipt), hasher)
		}
		return hashes
	}
	return peer.dispatchResponse(&Response{
		id:   res.RequestId,
		code: ReceiptsMsg,
		Res:  &res.ReceiptsResponse,
	}, metadata)
}

func handleNewPooledTransactionHashes67(backend Backend, msg Decoder, peer *Peer) error {
	// New transaction announcement arrived, make sure we have
	// a valid and fresh chain to handle them
	if !backend.AcceptTxs() {
		return nil
	}
	ann := new(NewPooledTransactionHashesPacket67)
	if err := msg.Decode(ann); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	// Schedule all the unknown hashes for retrieval
	for _, hash := range *ann {
		peer.markTransaction(hash)
	}
	return backend.Handle(peer, ann)
}

func handleNewPooledTransactionHashes68(backend Backend, msg Decoder, peer *Peer) error {
	// New transaction announcement arrived, make sure we have
	// a valid and fresh chain to handle them
	if !backend.AcceptTxs() {
		return nil
	}
	ann := new(NewPooledTransactionHashesPacket68)
	if err := msg.Decode(ann); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	if len(ann.Hashes) != len(ann.Types) || len(ann.Hashes) != len(ann.Sizes) {
		return fmt.Errorf("%w: message %v: invalid len of fields: %v %v %v", errDecode, msg, len(ann.Hashes), len(ann.Types), len(ann.Sizes))
	}
	// Schedule all the unknown hashes for retrieval
	for _, hash := range ann.Hashes {
		peer.markTransaction(hash)
	}
	return backend.Handle(peer, ann)
}

func handleGetPooledTransactions(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the pooled transactions retrieval message
	var query GetPooledTransactionsPacket
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	hashes, txs := answerGetPooledTransactions(backend, query.GetPooledTransactionsRequest)
	return peer.ReplyPooledTransactionsRLP(query.RequestId, hashes, txs)
}

func answerGetPooledTransactions(backend Backend, query GetPooledTransactionsRequest) ([]common.Hash, []rlp.RawValue) {
	// Gather transactions until the fetch or network limits is reached
	var (
		bytes  int
		hashes []common.Hash
		txs    []rlp.RawValue
	)
	for _, hash := range query {
		if bytes >= softResponseLimit {
			break
		}
		// Retrieve the requested transaction, skipping if unknown to us
		tx := backend.TxPool().Get(hash)
		if tx == nil {
			continue
		}
		// If known, encode and queue for response packet
		if encoded, err := rlp.EncodeToBytes(tx); err != nil {
			log.Error("Failed to encode transaction", "err", err)
		} else {
			hashes = append(hashes, hash)
			txs = append(txs, encoded)
			bytes += len(encoded)
		}
	}
	return hashes, txs
}

func handleTransactions(backend Backend, msg Decoder, peer *Peer) error {
	// Transactions arrived, make sure we have a valid and fresh chain to handle them
	if !backend.AcceptTxs() {
		return nil
	}
	// Transactions can be processed, parse all of them and deliver to the pool
	var txs TransactionsPacket
	if err := msg.Decode(&txs); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	for i, tx := range txs {
		// Validate and mark the remote transaction
		if tx == nil {
			return fmt.Errorf("%w: transaction %d is nil", errDecode, i)
		}
		peer.markTransaction(tx.Hash())
	}
	return backend.Handle(peer, &txs)
}

func handlePooledTransactions(backend Backend, msg Decoder, peer *Peer) error {
	// Transactions arrived, make sure we have a valid and fresh chain to handle them
	if !backend.AcceptTxs() {
		return nil
	}
	// Transactions can be processed, parse all of them and deliver to the pool
	var txs PooledTransactionsPacket
	if err := msg.Decode(&txs); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	for i, tx := range txs.PooledTransactionsResponse {
		// Validate and mark the remote transaction
		if tx == nil {
			return fmt.Errorf("%w: transaction %d is nil", errDecode, i)
		}
		peer.markTransaction(tx.Hash())
	}
	requestTracker.Fulfil(peer.id, peer.version, PooledTransactionsMsg, txs.RequestId)

	return backend.Handle(peer, &txs.PooledTransactionsResponse)
}

var fileDataReceiveTimes uint64

func handleFileDatas(backend Backend, msg Decoder, peer *Peer) error {
	// FileDatas can be processed, parse all of them and deliver to the pool
	var fds FileDataPacket
	if err := msg.Decode(&fds); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}

	flag := peer.knownFds.Contains(fds[0].TxHash)
	if flag {
		return nil
	}

	fileDataReceiveTimes++

	for i, fd := range fds {
		// Validate and mark the remote fileData
		if fd == nil {
			return fmt.Errorf("%w: fileData %d is nil", errDecode, i)
		}
		log.Info("handleFileDatas----", "TxHash", fd.TxHash)
		peer.markFileData(fd.TxHash)
	}
	log.Info("handleFileDatas----收到了FileDataPacket", "fileDataReceiveTimes", fileDataReceiveTimes)
	return backend.Handle(peer, &fds)
}

func handleGetPooledFileDatas(backend Backend, msg Decoder, peer *Peer) error {
	var query GetPooledFileDataPacket
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	log.Info("handleGetPooledFileDatas----获取要拿的请求", "query hash", query.GetPooledFileDatasRequest[0].String())
	hashes, fds, status := answerGetPooledFileDatas(backend, query.GetPooledFileDatasRequest)
	return peer.ReplyPooledFileDatasRLP(query.RequestId, hashes, fds, status)
}

func answerGetPooledFileDatas(backend Backend, query GetPooledFileDatasRequest) ([]common.Hash, []rlp.RawValue, []uint) {
	// Gather fileDatas until the fetch or network limits is reached
	var (
		bytes  int
		hashes []common.Hash
		fds    []rlp.RawValue
		states []uint
	)
	for _, hash := range query {
		// Retrieve the requested fileData, skipping if unknown to us
		fd, state, err := backend.FildDataPool().Get(hash)
		if err != nil {
			continue
		}

		switch state {
		case filedatapool.DISK_FILEDATA_STATE_DEL:
			states = append(states, 0)
		case filedatapool.DISK_FILEDATA_STATE_SAVE:
			states = append(states, 1)
		case filedatapool.DISK_FILEDATA_STATE_UNKNOW:
			states = append(states, 2)
		}

		if fd != nil {
			// If known, encode and queue for response packet
			if encoded, err := rlp.EncodeToBytes(fd); err != nil {
				log.Error("Failed to encode transaction", "err", err)
			} else {
				hashes = append(hashes, hash)
				fds = append(fds, encoded)
				bytes += len(encoded)
			}
		}
	}
	return hashes, fds, states
}

func handleNewPooledFileDataHashes67(backend Backend, msg Decoder, peer *Peer) error {
	ann := new(NewPooledFileDataHashesPacket67)
	if err := msg.Decode(ann); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	// Schedule all the unknown hashes for retrieval
	for _, hash := range *ann {
		log.Info("handleNewPooledFileDataHashes67---收到了交易哈希", "txHash", hash.String())
		peer.markFileData(hash)
	}
	return backend.Handle(peer, ann)
}

func handleNewPooledFileDataHashes68(backend Backend, msg Decoder, peer *Peer) error {
	ann := new(NewPooledFileDataHashesPacket68)
	if err := msg.Decode(ann); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	if len(ann.Hashes) != len(ann.Sizes) {
		return fmt.Errorf("%w: message %v: invalid len of fields: %v %v", errDecode, msg, len(ann.Hashes), len(ann.Sizes))
	}
	// Schedule all the unknown hashes for retrieval
	for _, hash := range ann.Hashes {
		log.Info("handleNewPooledFileDataHashes68---收到了交易哈希", "txHash", hash.String())
		peer.markFileData(hash)
	}
	return backend.Handle(peer, ann)
}

func handlePooledFileDatas(backend Backend, msg Decoder, peer *Peer) error {
	// FileDatas can be processed, parse all of them and deliver to the pool
	var fds PooledFileDataPacket
	if err := msg.Decode(&fds); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}

	for i, fd := range fds.PooledFileDataResponse {
		// Validate and mark the remote fileData
		if fd == nil {
			return fmt.Errorf("%w: fileData %d is nil", errDecode, i)
		}
		log.Info("handlePooledFileDatas----", "txHash", fd.TxHash.String())
		peer.markFileData(fd.TxHash)
	}
	requestTracker.Fulfil(peer.id, peer.version, PooledFileDatasMsg, fds.RequestId)
	return backend.Handle(peer, &fds.PooledFileDataResponse)
}

func handleResFileDatas(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of fileDatas arrived to one of our previous requests
	res := new(FileDatasResponseRLPPacket)
	if err := msg.Decode(res); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}

	metaData := func() interface{} {
		var btfd BantchFileData
		err := rlp.DecodeBytes(res.FileDatasResponse, &btfd)
		if err != nil {
			log.Error("handleResFileDatas----decode BantchFileData err", "err", err.Error())
		}
		hashes := make([]common.Hash, len(btfd.FileDatas))
		for inde, data := range btfd.FileDatas {
			var fd types.FileData
			rlp.DecodeBytes(data, &fd)
			hashes[inde] = fd.TxHash
		}
		return hashes
	}

	return peer.dispatchResponse(&Response{
		id:   res.RequestId,
		code: ResFileDatasMsg,
		Res:  &res.FileDatasResponse,
	}, metaData)
}

func handleReqFileDatas(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the block fileDatas retrieval message
	var query GetFileDatasPacket
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	response := ServiceGetFileDatasQuery(backend.Chain(), query.GetFileDatasRequest)
	errs := peer.ReplyFileDatasMarshal(query.RequestId, response)
	if len(errs) != 0 {
		return errors.New("send Requested FileDatas failed")
	} else {
		return nil
	}
}

type BantchFileData struct {
	HeaderHash common.Hash `json:"headerhash"`
	Cap        uint64      `json:"cap"`
	Length     uint64      `json:"length"`
	FileDatas  [][]byte    `json:"filedatas"`
}

// ServiceGetFileDatasQuery assembles the response to a fileData query. It is
// exposed to allow external packages to test protocol behavior.
func ServiceGetFileDatasQuery(chain *core.BlockChain, query GetFileDatasRequest) []*BantchFileData {
	// Gather state data until the fetch or network limits is reached
	var (
		bytes int
	)

	var batch uint64
	var cap uint64

	resultList := make([]*BantchFileData, 0)

	for _, hash := range query {
		// Retrieve the requested block's fileData
		results := chain.GetFileDatasByHash(hash)
		if results == nil {
			if header := chain.GetHeaderByHash(hash); header == nil {
				continue
			}
		}

		// how many batch of fileDatas should send by one header hash
		cap = uint64(len(results))
		batchFileDatas := make([][][]byte, 0)
		for index, fd := range results {
			encoded, err := rlp.EncodeToBytes(fd)
			if err != nil {
				log.Error("Failed to encode fileData", "err", err)
			} else {
				bytes += len(encoded)
				if bytes >= fileDataSoftResponseLimit || len(batchFileDatas[batch]) >= maxFileDatasServe {
					batch++
				}

				list := batchFileDatas[index]
				if list == nil {
					list = make([][]byte, 0)
				} else {
					list = append(list, encoded)
				}
				batchFileDatas[index] = list
			}
		}

		//
		for _, datas := range batchFileDatas {
			btFD := &BantchFileData{
				HeaderHash: hash,
				Cap:        cap,
				Length:     uint64(len(datas)),
				FileDatas:  datas,
			}
			resultList = append(resultList, btFD)
		}

	}
	return resultList
}
