/**
 * Copyright 2023.12.28
 * @Author: EchoWu
 * @Description: This file is part of the DOMICON library.
 */
package downloader

import (
	"time"

	"github.com/domicon-labs/op-geth/common"
	"github.com/domicon-labs/op-geth/eth/protocols/eth"
	"github.com/domicon-labs/op-geth/log"
)

// fileDataQueue implements typedQueue and is a type adapter between the generic
// concurrent fetcher and the downloader.
type fileDataQueue Downloader

// waker returns a notification channel that gets pinged in case more fileData
// fetches have been queued up, so the fetcher might assign it to idle peers.
func (q *fileDataQueue) waker() chan bool {
	return q.queue.fileDataWakeCh
}

// pending returns the number of fileData that are currently queued for fetching
// by the concurrent downloader.
func (q *fileDataQueue) pending() int {
	return q.queue.PendingFileDatas()
}

// capacity is responsible for calculating how many fileDatas a particular peer is
// estimated to be able to retrieve within the allotted round trip time.
func (q *fileDataQueue) capacity(peer *peerConnection, rtt time.Duration) int {
	return peer.FileDataCapacity(rtt)
}

// updateCapacity is responsible for updating how many receipts a particular peer
// is estimated to be able to retrieve in a unit time.
func (q *fileDataQueue) updateCapacity(peer *peerConnection, items int, span time.Duration) {
	peer.UpdateFileDataRate(items, span)
}

// reserve is responsible for allocating a requested number of pending receipts
// from the download queue to the specified peer.
func (q *fileDataQueue) reserve(peer *peerConnection, items int) (*fetchRequest, bool, bool) {
	return q.queue.ReserveFileDatas(peer, items)
}

// unreserve is responsible for removing the current fileData retrieval allocation
// assigned to a specific peer and placing it back into the pool to allow
// reassigning to some other peer.
func (q *fileDataQueue) unreserve(peer string) int {
	fails := q.queue.ExpireFileDatas(peer)
	if fails > 2 {
		log.Trace("Receipt delivery timed out", "peer", peer)
	} else {
		log.Debug("Receipt delivery stalling", "peer", peer)
	}
	return fails
}

// request is responsible for converting a generic fetch request into a fileData
// one and sending it to the remote peer for fulfillment.
func (q *fileDataQueue) request(peer *peerConnection, req *fetchRequest, resCh chan *eth.Response) (*eth.Request, error) {
	peer.log.Trace("Requesting new batch of fileData", "count", len(req.Headers), "from", req.Headers[0].Number)
	if q.fileDataFetchHook != nil {
		q.fileDataFetchHook(req.Headers)
	}
	hashes := make([]common.Hash, 0, len(req.Headers))
	for _, header := range req.Headers {
		hashes = append(hashes, header.Hash())
	}
	return peer.peer.StartRequestFileDatas(hashes, resCh)
}

// deliver is responsible for taking a generic response packet from the concurrent
// fetcher, unpacking the fileData data and delivering it to the downloader's queue.
func (q *fileDataQueue) deliver(peer *peerConnection, packet *eth.Response) (int, error) {
	fileDatas := *packet.Res.(*eth.PooledFileDataResponse)
	//receipts := *packet.Res.(*eth.ReceiptsResponse)
	hashes := packet.Meta.([]common.Hash) // {fileDatas hashes}

	accepted, err := q.queue.DeliverFileDatas(peer.id, fileDatas, hashes)
	switch {
	case err == nil && len(fileDatas) == 0:
		peer.log.Trace("Requested receipts delivered")
	case err == nil:
		peer.log.Trace("Delivered new batch of fileData", "count", len(fileDatas), "accepted", accepted)
	default:
		peer.log.Debug("Failed to deliver retrieved receipts", "err", err)
	}
	return accepted, err
}
