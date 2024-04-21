package fetcher

import (
	"errors"
	"fmt"
	"math"
	mrand "math/rand"
	"sort"
	"time"

	"github.com/domicon-labs/op-geth/common"
	"github.com/domicon-labs/op-geth/common/mclock"
	"github.com/domicon-labs/op-geth/core/txpool/filedatapool"
	"github.com/domicon-labs/op-geth/core/types"
	"github.com/domicon-labs/op-geth/log"
	"github.com/domicon-labs/op-geth/metrics"
)

var (
	// maxFdAnnounces is the maximum number of unique fileData a peer
	// can announce in a short time.
	maxFdAnnounces = 4096

	// maxFdRetrievals is the maximum number of fileData that can be fetched
	// in one request. The rationale for picking 256 is to have a reasonabe lower
	// bound for the transferred data (don't waste RTTs, transfer more meaningful
	// batch sizes), but also have an upper bound on the sequentiality to allow
	// using our entire peerset for deliveries.
	//
	// This number also acts as a failsafe against malicious announces which might
	// cause us to request more data than we'd expect.
	maxFdRetrievals = 256

	// maxFdRetrievalSize is the max number of bytes that delivered fileData
	// should weigh according to the announcements. The 500KB was chosen to limit
	// retrieving a maximum of one fileData at a time to minimize hogging
	// a connection between two peers.
	maxFdRetrievalSize = 500 * 1024

	// fdGatherSlack is the interval used to collate almost-expired announces
	// with network fetches.
	fdGatherSlack = 100 * time.Millisecond

	// fdArriveTimeout is the time allowance before an announced transaction is
	// explicitly requested.
	fdArriveTimeout = 500 * time.Millisecond
	// fdFetchTimeout is the maximum allotted time to return an explicitly
	// requested fileData.
	fdFetchTimeout = 5 * time.Second
)

var (
	fdAnnounceInMeter    = metrics.NewRegisteredMeter("eth/fetcher/filedata/announces/in", nil)
	fdAnnounceKnownMeter = metrics.NewRegisteredMeter("eth/fetcher/filedata/announces/known", nil)
	fdAnnounceDOSMeter   = metrics.NewRegisteredMeter("eth/fetcher/filedata/announces/dos", nil)

	fdBroadcastInMeter          = metrics.NewRegisteredMeter("eth/fetcher/filedata/broadcasts/in", nil)
	fdBroadcastKnownMeter       = metrics.NewRegisteredMeter("eth/fetcher/filedata/broadcasts/known", nil)
	fdBroadcastOtherRejectMeter = metrics.NewRegisteredMeter("eth/fetcher/filedata/broadcasts/otherreject", nil)

	fdRequestOutMeter     = metrics.NewRegisteredMeter("eth/fetcher/filedata/request/out", nil)
	fdRequestFailMeter    = metrics.NewRegisteredMeter("eth/fetcher/filedata/request/fail", nil)
	fdRequestDoneMeter    = metrics.NewRegisteredMeter("eth/fetcher/filedata/request/done", nil)
	fdRequestTimeoutMeter = metrics.NewRegisteredMeter("eth/fetcher/filedata/request/timeout", nil)

	fdReplyInMeter          = metrics.NewRegisteredMeter("eth/fetcher/filedata/replies/in", nil)
	fdReplyKnownMeter       = metrics.NewRegisteredMeter("eth/fetcher/filedata/replies/known", nil)
	fdReplyOtherRejectMeter = metrics.NewRegisteredMeter("eth/fetcher/filedata/replies/otherreject", nil)

	fdFetcherWaitingPeers  = metrics.NewRegisteredGauge("eth/fetcher/filedata/waiting/peers", nil)
	fdFetcherWaitingHashes = metrics.NewRegisteredGauge("eth/fetcher/filedata/waiting/hashes", nil)
	//fdFetcherQueueingPeers  = metrics.NewRegisteredGauge("eth/fetcher/filedata/queueing/peers", nil)
	//fdFetcherQueueingHashes = metrics.NewRegisteredGauge("eth/fetcher/filedata/queueing/hashes", nil)
	fdFetcherFetchingPeers  = metrics.NewRegisteredGauge("eth/fetcher/filedata/fetching/peers", nil)
	fdFetcherFetchingHashes = metrics.NewRegisteredGauge("eth/fetcher/filedata/fetching/hashes", nil)
)

// fdDelivery is the notification that a batch of fileDatas have been added
// to the pool and should be untracked.
type fdDelivery struct {
	origin string        // Identifier of the peer originating the notification
	hashes []common.Hash // Batch of fileDatas hashes having been delivered
	metas  []fdMetadata  // Batch of metadatas associated with the delivered hashes
	direct bool          // Whether this is a direct reply or a broadcast
}

// fdAnnounce is the notification of the availability of a batch
// of new fileData in the network.
type fdAnnounce struct {
	origin string        // Identifier of the peer originating the notification
	hashes []common.Hash // Batch of fileData hashes being announced
	metas  []*fdMetadata // Batch of metadatas associated with the hashes (nil before eth/68)
}

// txMetadata is a set of extra data transmitted along the announcement for better
// fetch scheduling.
type fdMetadata struct {
	size uint32 // FileData size in bytes
}

// fdRequest represents an in-flight transaction retrieval request destined to
// a specific peers.
type fdRequest struct {
	hashes []common.Hash            // FileData having been requested
	stolen map[common.Hash]struct{} // Deliveries by someone else (don't re-request)
	time   mclock.AbsTime           // Timestamp of the request
}

// fdDrop is the notification that a peer has disconnected.
type fdDrop struct {
	peer string
}

type FileDataFetcher struct {
	notify  chan *fdAnnounce
	cleanup chan *fdDelivery
	drop    chan *fdDrop
	quit    chan struct{}

	// Stage 1: Waiting lists for newly discovered fileDatas that might be
	// broadcast without needing explicit request/reply round trips.
	waitlist  map[common.Hash]map[string]struct{}    // fileDatas waiting for an potential broadcast
	waittime  map[common.Hash]mclock.AbsTime         // Timestamps when transactions were added to the waitlist
	waitslots map[string]map[common.Hash]*fdMetadata // Waiting announcements grouped by peer (DoS protection)

	// Stage 2: Queue of fileDatas that waiting to be allocated to some peer
	// to be retrieved directly.
	announces map[string]map[common.Hash]*fdMetadata // Set of announced fileData, grouped by origin peer
	announced map[common.Hash]map[string]struct{}    // Set of download locations, grouped by fileData hash

	// Stage 3: Set of fileDatas currently being retrieved, some which may be
	// fulfilled and some rescheduled. Note, this step shares 'announces' from the
	// previous stage to avoid having to duplicate (need it for DoS checks).
	fetching   map[common.Hash]string              // Transaction set currently being retrieved
	requests   map[string]*fdRequest               // In-flight fileData retrievals
	alternates map[common.Hash]map[string]struct{} // In-flight fileData alternate origins if retrieval fails

	// Callbacks
	hasFd    func(common.Hash) bool            // Retrieves a fd from the local fdpool
	addFds   func([]*types.FileData) []error   // Insert a batch of fileData into local fdpool
	fetchFds func(string, []common.Hash) error // Retrieves a set of fileData from a remote peer
	dropPeer func(string)                      // Drops a peer in case of announcement violation

	step  chan struct{} // Notification channel when the fetcher loop iterates
	clock mclock.Clock  // Time wrapper to simulate in tests
	rand  *mrand.Rand   // Randomizer to use in tests instead of map range loops (soft-random)
}

// NewFdFetcher creates a transaction fetcher to retrieve transaction
// based on hash announcements.
func NewFdFetcher(hasFd func(common.Hash) bool, addFds func([]*types.FileData) []error, fetchFds func(string, []common.Hash) error, dropPeer func(string)) *FileDataFetcher {
	return NewFdFetcherForTests(hasFd, addFds, fetchFds, dropPeer, mclock.System{}, nil)
}

// NewFdFetcherForTests is a testing method to mock out the realtime clock with
// a simulated version and the internal randomness with a deterministic one.
func NewFdFetcherForTests(
	hasFd func(common.Hash) bool, addFds func([]*types.FileData) []error, fetchFds func(string, []common.Hash) error, dropPeer func(string),
	clock mclock.Clock, rand *mrand.Rand) *FileDataFetcher {
	return &FileDataFetcher{
		notify:     make(chan *fdAnnounce),
		cleanup:    make(chan *fdDelivery),
		drop:       make(chan *fdDrop),
		quit:       make(chan struct{}),
		waitlist:   make(map[common.Hash]map[string]struct{}),
		waittime:   make(map[common.Hash]mclock.AbsTime),
		waitslots:  make(map[string]map[common.Hash]*fdMetadata),
		announces:  make(map[string]map[common.Hash]*fdMetadata),
		announced:  make(map[common.Hash]map[string]struct{}),
		fetching:   make(map[common.Hash]string),
		requests:   make(map[string]*fdRequest),
		alternates: make(map[common.Hash]map[string]struct{}),
		hasFd:      hasFd,
		addFds:     addFds,
		fetchFds:   fetchFds,
		dropPeer:   dropPeer,
		clock:      clock,
		rand:       rand,
	}
}

// Notify announces the fetcher of the potential availability of a new batch of
// fileDatas in the network.
func (f *FileDataFetcher) Notify(peer string, types []byte, sizes []uint32, hashes []common.Hash) error {
	// Keep track of all the announced fileDatas
	fdAnnounceInMeter.Mark(int64(len(hashes)))

	// Skip any fileData announcements that we already know of, or that we've
	// previously marked as cheap and discarded. This check is of course racy,
	// because multiple concurrent notifies will still manage to pass it, but it's
	// still valuable to check here because it runs concurrent  to the internal
	// loop, so anything caught here is time saved internally.
	var (
		unknownHashes = make([]common.Hash, 0, len(hashes))
		unknownMetas  = make([]*fdMetadata, 0, len(hashes))

		duplicate int64
	)
	for i, hash := range hashes {
		switch {
		case f.hasFd(hash):
			duplicate++

		default:
			unknownHashes = append(unknownHashes, hash)
			unknownMetas = append(unknownMetas, &fdMetadata{size: sizes[i]})
		}
	}
	fdAnnounceKnownMeter.Mark(duplicate)
	// If anything's left to announce, push it into the internal loop
	if len(unknownHashes) == 0 {
		return nil
	}
	announce := &fdAnnounce{origin: peer, hashes: unknownHashes, metas: unknownMetas}
	select {
	case f.notify <- announce:
		return nil
	case <-f.quit:
		return errTerminated
	}
}

// Enqueue imports a batch of received FileData into the FileData pool
// and the fetcher. This method may be called by both FileData broadcasts and
// direct request replies. The differentiation is important so the fetcher can
// re-schedule missing FileData as soon as possible.
func (f *FileDataFetcher) Enqueue(peer string, fds []*types.FileData, direct bool) error {
	var (
		inMeter          = fdReplyInMeter
		knownMeter       = fdReplyKnownMeter
		otherRejectMeter = fdReplyOtherRejectMeter
	)
	if !direct {
		inMeter = fdBroadcastInMeter
		knownMeter = fdBroadcastKnownMeter
		otherRejectMeter = fdBroadcastOtherRejectMeter
	}

	// Keep track of all the propagated fileData
	inMeter.Mark(int64(len(fds)))

	// Push all the fileDate into the pool
	var (
		added = make([]common.Hash, 0, len(fds))
		metas = make([]fdMetadata, 0, len(fds))
	)
	// proceed in batches
	for i := 0; i < len(fds); i += 128 {
		end := i + 128
		if end > len(fds) {
			end = len(fds)
		}
		var (
			duplicate   int64
			otherreject int64
		)
		batch := fds[i:end]

		for j, err := range f.addFds(batch) {
			// Track a few interesting failure types
			switch {
			case err == nil: // Noop, but need to handle to not count these

			case errors.Is(err, filedatapool.ErrAlreadyKnown):
				duplicate++

			default:
				otherreject++
			}
			added = append(added, batch[j].TxHash)
			met := fdMetadata{size: uint32(batch[j].Size())}
			metas = append(metas, met)
		}
		knownMeter.Mark(duplicate)
		otherRejectMeter.Mark(otherreject)

		// If 'other reject' is >25% of the deliveries in any batch, sleep a bit.
		if otherreject > 128/4 {
			time.Sleep(200 * time.Millisecond)
			log.Warn("Peer delivering stale transactions", "peer", peer, "rejected", otherreject)
		}
	}

	select {
	case f.cleanup <- &fdDelivery{origin: peer, hashes: added, metas: metas, direct: direct}:
		return nil
	case <-f.quit:
		return errTerminated
	}
}

// Drop should be called when a peer disconnects. It cleans up all the internal
// data structures of the given node.
func (f *FileDataFetcher) Drop(peer string) error {
	select {
	case f.drop <- &fdDrop{peer: peer}:
		return nil
	case <-f.quit:
		return errTerminated
	}
}

// Start boots up the announcement based synchroniser, accepting and processing
// hash notifications and block fetches until termination requested.
func (f *FileDataFetcher) Start() {
	go f.loop()
}

// Stop terminates the announcement based synchroniser, canceling all pending
// operations.
func (f *FileDataFetcher) Stop() {
	close(f.quit)
}

func (f *FileDataFetcher) loop() {
	var (
		waitTimer    = new(mclock.Timer)
		timeoutTimer = new(mclock.Timer)

		waitTrigger    = make(chan struct{}, 1)
		timeoutTrigger = make(chan struct{}, 1)
	)
	for {
		select {
		case ann := <-f.notify:
			// Drop part of the new announcements if there are too many accumulated.
			// Note, we could but do not filter already known fileData here as
			// the probability of something arriving between this call and the pre-
			// filter outside is essentially zero.
			used := len(f.waitslots[ann.origin]) + len(f.announces[ann.origin])
			if used >= maxFdAnnounces {
				// This can happen if a set of fileDatas are requested but not
				// all fulfilled, so the remainder are rescheduled without the cap
				// check. Should be fine as the limit is in the thousands and the
				// request size in the hundreds.
				fdAnnounceDOSMeter.Mark(int64(len(ann.hashes)))
				break
			}
			want := used + len(ann.hashes)
			if want > maxFdAnnounces {
				fdAnnounceDOSMeter.Mark(int64(want - maxFdAnnounces))

				ann.hashes = ann.hashes[:want-maxFdAnnounces]
				ann.metas = ann.metas[:want-maxFdAnnounces]
			}
			// All is well, schedule the remainder of the fileData
			idleWait := len(f.waittime) == 0
			_, oldPeer := f.announces[ann.origin]
			for i, hash := range ann.hashes {
				// If the fileData is already downloading, add it to the list
				// of possible alternates (in case the current retrieval fails) and
				// also account it for the peer.
				if f.alternates[hash] != nil {
					f.alternates[hash][ann.origin] = struct{}{}

					// Stage 2 and 3 share the set of origins per tx
					if announces := f.announces[ann.origin]; announces != nil {
						announces[hash] = ann.metas[i]
					} else {
						f.announces[ann.origin] = map[common.Hash]*fdMetadata{hash: ann.metas[i]}
					}
					continue
				}
				// If the fileData is not downloading, but is already queued
				// from a different peer, track it for the new peer too.
				if f.announced[hash] != nil {
					f.announced[hash][ann.origin] = struct{}{}

					// Stage 2 and 3 share the set of origins per fd
					if announces := f.announces[ann.origin]; announces != nil {
						announces[hash] = ann.metas[i]
					} else {
						f.announces[ann.origin] = map[common.Hash]*fdMetadata{hash: ann.metas[i]}
					}
					continue
				}

				// If the fileDatas is already known to the fetcher, but not
				// yet downloading, add the peer as an alternate origin in the
				// waiting list.
				if f.waitlist[hash] != nil {
					// Ignore double announcements from the same peer. This is
					// especially important if metadata is also passed along to
					// prevent malicious peers flip-flopping good/bad values.
					if _, ok := f.waitlist[hash][ann.origin]; ok {
						continue
					}
					f.waitlist[hash][ann.origin] = struct{}{}

					if waitslots := f.waitslots[ann.origin]; waitslots != nil {
						waitslots[hash] = ann.metas[i]
					} else {
						f.waitslots[ann.origin] = map[common.Hash]*fdMetadata{hash: ann.metas[i]}
					}
					continue
				}
				// fileDatas unknown to the fetcher, insert it into the waiting list
				f.waitlist[hash] = map[string]struct{}{ann.origin: {}}
				f.waittime[hash] = f.clock.Now()

				if waitslots := f.waitslots[ann.origin]; waitslots != nil {
					waitslots[hash] = ann.metas[i]
				} else {
					f.waitslots[ann.origin] = map[common.Hash]*fdMetadata{hash: ann.metas[i]}
				}
			}
			// If a new item was added to the waitlist, schedule it into the fetcher
			if idleWait && len(f.waittime) > 0 {
				f.rescheduleWait(waitTimer, waitTrigger)
			}
			// If this peer is new and announced something already queued, maybe
			// request fileDatas from them
			if oldPeer && len(f.announces[ann.origin]) > 0 {
				log.Info("FileDataFetcher---loop--去要了")
				f.scheduleFetches(timeoutTimer, timeoutTrigger, map[string]struct{}{ann.origin: {}})
			}

		case <-waitTrigger:
			// At least one fileData's waiting time ran out, push all expired
			// ones into the retrieval queues
			actives := make(map[string]struct{})
			for hash, instance := range f.waittime {
				if time.Duration(f.clock.Now()-instance)+fdGatherSlack > fdArriveTimeout {
					// Transaction expired without propagation, schedule for retrieval
					if f.announced[hash] != nil {
						panic("announce tracker already contains waitlist item")
					}
					f.announced[hash] = f.waitlist[hash]
					for peer := range f.waitlist[hash] {
						if announces := f.announces[peer]; announces != nil {
							announces[hash] = f.waitslots[peer][hash]
						} else {
							f.announces[peer] = map[common.Hash]*fdMetadata{hash: f.waitslots[peer][hash]}
						}
						delete(f.waitslots[peer], hash)
						if len(f.waitslots[peer]) == 0 {
							delete(f.waitslots, peer)
						}
						actives[peer] = struct{}{}
					}
					delete(f.waittime, hash)
					delete(f.waitlist, hash)
				}
			}
			// If transactions are still waiting for propagation, reschedule the wait timer
			if len(f.waittime) > 0 {
				f.rescheduleWait(waitTimer, waitTrigger)
			}
			// If any peers became active and are idle, request fileData from them
			if len(actives) > 0 {
				f.scheduleFetches(timeoutTimer, timeoutTrigger, actives)
			}

		case <-timeoutTrigger:
			// Clean up any expired retrievals and avoid re-requesting them from the
			// same peer (either overloaded or malicious, useless in both cases). We
			// could also penalize (Drop), but there's nothing to gain, and if could
			// possibly further increase the load on it.
			for peer, req := range f.requests {
				if time.Duration(f.clock.Now()-req.time)+100*time.Millisecond > fdFetchTimeout {
					fdRequestTimeoutMeter.Mark(int64(len(req.hashes)))
					// Reschedule all the not-yet-delivered fetches to alternate peers
					for _, hash := range req.hashes {
						// Skip rescheduling hashes already delivered by someone else
						if req.stolen != nil {
							if _, ok := req.stolen[hash]; ok {
								continue
							}
						}
						// Move the delivery back from fetching to queued
						if _, ok := f.announced[hash]; ok {
							panic("announced tracker already contains alternate item")
						}
						if f.alternates[hash] != nil { // nil if tx was broadcast during fetch
							f.announced[hash] = f.alternates[hash]
						}
						delete(f.announced[hash], peer)
						if len(f.announced[hash]) == 0 {
							delete(f.announced, hash)
						}
						//delete(f.announces[peer], hash)
						delete(f.alternates, hash)
						delete(f.fetching, hash)
					}
					if len(f.announces[peer]) == 0 {
						delete(f.announces, peer)
					}
					// Keep track of the request as dangling, but never expire
					f.requests[peer].hashes = nil
				}
			}
			// Schedule a new transaction retrieval
			f.scheduleFetches(timeoutTimer, timeoutTrigger, nil)

			// No idea if we scheduled something or not, trigger the timer if needed
			// TODO(karalabe): this is kind of lame, can't we dump it into scheduleFetches somehow?
			f.rescheduleTimeout(timeoutTimer, timeoutTrigger)

		case delivery := <-f.cleanup:
			// Independent if the delivery was direct or broadcast, remove all
			// traces of the hash from internal trackers. That said, compare any
			// advertised metadata with the real ones and drop bad peers.
			for i, hash := range delivery.hashes {
				if _, ok := f.waitlist[hash]; ok {
					for peer, fdset := range f.waitslots {
						if meta := fdset[hash]; meta != nil {
							if delivery.metas[i].size != meta.size {
								if math.Abs(float64(delivery.metas[i].size)-float64(meta.size)) > 8 {
									log.Warn("Announced fileData size mismatch", "peer", peer, "tx", hash, "size", delivery.metas[i].size, "ann", meta.size)
									// Normally we should drop a peer considering this is a protocol violation.
									// However, due to the RLP vs consensus format messyness, allow a few bytes
									// wiggle-room where we only warn, but don't drop.
									//
									// TODO(karalabe): Get rid of this relaxation when clients are proven stable.
									f.dropPeer(peer)
								}
							}
						}
						delete(fdset, hash)
						if len(fdset) == 0 {
							delete(f.waitslots, peer)
						}
					}
					delete(f.waitlist, hash)
					delete(f.waittime, hash)
				} else {
					for peer, fdset := range f.announces {
						if meta := fdset[hash]; meta != nil {
							if delivery.metas[i].size != meta.size {
								if math.Abs(float64(delivery.metas[i].size)-float64(meta.size)) > 8 {
									log.Warn("Announced fileData size mismatch", "peer", peer, "tx", hash, "size", delivery.metas[i].size, "ann", meta.size)

									// Normally we should drop a peer considering this is a protocol violation.
									// However, due to the RLP vs consensus format messyness, allow a few bytes
									// wiggle-room where we only warn, but don't drop.
									//
									// TODO(karalabe): Get rid of this relaxation when clients are proven stable.
									f.dropPeer(peer)
								}
							}
						}
						delete(fdset, hash)
						if len(fdset) == 0 {
							delete(f.announces, peer)
						}
					}
					delete(f.announced, hash)
					delete(f.alternates, hash)

					// If a transaction currently being fetched from a different
					// origin was delivered (delivery stolen), mark it so the
					// actual delivery won't double schedule it.
					if origin, ok := f.fetching[hash]; ok && (origin != delivery.origin || !delivery.direct) {
						stolen := f.requests[origin].stolen
						if stolen == nil {
							f.requests[origin].stolen = make(map[common.Hash]struct{})
							stolen = f.requests[origin].stolen
						}
						stolen[hash] = struct{}{}
					}
					delete(f.fetching, hash)
				}
			}

			// In case of a direct delivery, also reschedule anything missing
			// from the original query
			if delivery.direct {
				// Mark the requesting successful (independent of individual status)
				fdRequestDoneMeter.Mark(int64(len(delivery.hashes)))

				// Make sure something was pending, nuke it
				req := f.requests[delivery.origin]
				if req == nil {
					log.Warn("Unexpected fileData delivery", "peer", delivery.origin)
					break
				}
				delete(f.requests, delivery.origin)

				// Anything not delivered should be re-scheduled (with or without
				// this peer, depending on the response cutoff)
				delivered := make(map[common.Hash]struct{})
				for _, hash := range delivery.hashes {
					delivered[hash] = struct{}{}
				}
				cutoff := len(req.hashes) // If nothing is delivered, assume everything is missing, don't retry!!!
				for i, hash := range req.hashes {
					if _, ok := delivered[hash]; ok {
						cutoff = i
					}
				}
				// Reschedule missing hashes from alternates, not-fulfilled from alt+self
				for i, hash := range req.hashes {
					// Skip rescheduling hashes already delivered by someone else
					if req.stolen != nil {
						if _, ok := req.stolen[hash]; ok {
							continue
						}
					}
					if _, ok := delivered[hash]; !ok {
						if i < cutoff {
							delete(f.alternates[hash], delivery.origin)
							delete(f.announces[delivery.origin], hash)
							if len(f.announces[delivery.origin]) == 0 {
								delete(f.announces, delivery.origin)
							}
						}
						if len(f.alternates[hash]) > 0 {
							if _, ok := f.announced[hash]; ok {
								panic(fmt.Sprintf("announced tracker already contains alternate item: %v", f.announced[hash]))
							}
							f.announced[hash] = f.alternates[hash]
						}
					}
					delete(f.alternates, hash)
					delete(f.fetching, hash)
				}

				// Something was delivered, try to reschedule requests
				f.scheduleFetches(timeoutTimer, timeoutTrigger, nil) // Partial delivery may enable others to deliver too
			}

		case drop := <-f.drop:
			// Clean up any active requests
			var request *fdRequest
			if request = f.requests[drop.peer]; request != nil {
				for _, hash := range request.hashes {
					// Skip rescheduling hashes already delivered by someone else
					if request.stolen != nil {
						if _, ok := request.stolen[hash]; ok {
							continue
						}
					}
					// Undelivered hash, reschedule if there's an alternative origin available
					delete(f.alternates[hash], drop.peer)
					if len(f.alternates[hash]) == 0 {
						delete(f.alternates, hash)
					} else {
						f.announced[hash] = f.alternates[hash]
						delete(f.alternates, hash)
					}
					delete(f.fetching, hash)
				}
				delete(f.requests, drop.peer)
			}

			// If a request was cancelled, check if anything needs to be rescheduled
			if request != nil {
				f.scheduleFetches(timeoutTimer, timeoutTrigger, nil)
				f.rescheduleTimeout(timeoutTimer, timeoutTrigger)
			}

		case <-f.quit:
			return
		}
		// No idea what happened, but bump some sanity metrics
		fdFetcherWaitingHashes.Update(int64(len(f.waitlist)))
		//txFetcherQueueingHashes.Update(int64(len(f.announced)))
		fdFetcherFetchingPeers.Update(int64(len(f.requests)))
		fdFetcherFetchingHashes.Update(int64(len(f.fetching)))

		// Loop did something, ping the step notifier if needed (tests)
		if f.step != nil {
			f.step <- struct{}{}
		}
	}
}

// rescheduleWait iterates over all the transactions currently in the waitlist
// and schedules the movement into the fetcher for the earliest.
//
// The method has a granularity of 'gatherSlack', since there's not much point in
// spinning over all the transactions just to maybe find one that should trigger
// a few ms earlier.
func (f *FileDataFetcher) rescheduleWait(timer *mclock.Timer, trigger chan struct{}) {
	if *timer != nil {
		(*timer).Stop()
	}
	now := f.clock.Now()

	earliest := now
	for _, instance := range f.waittime {
		if earliest > instance {
			earliest = instance
			if txArriveTimeout-time.Duration(now-earliest) < gatherSlack {
				break
			}
		}
	}
	*timer = f.clock.AfterFunc(txArriveTimeout-time.Duration(now-earliest), func() {
		trigger <- struct{}{}
	})
}

// rescheduleTimeout iterates over all the transactions currently in flight and
// schedules a cleanup run when the first would trigger.
//
// The method has a granularity of 'gatherSlack', since there's not much point in
// spinning over all the transactions just to maybe find one that should trigger
// a few ms earlier.
//
// This method is a bit "flaky" "by design". In theory the timeout timer only ever
// should be rescheduled if some request is pending. In practice, a timeout will
// cause the timer to be rescheduled every 5 secs (until the peer comes through or
// disconnects). This is a limitation of the fetcher code because we don't trac
// pending requests and timed out requests separately. Without double tracking, if
// we simply didn't reschedule the timer on all-timeout then the timer would never
// be set again since len(request) > 0 => something's running.
func (f *FileDataFetcher) rescheduleTimeout(timer *mclock.Timer, trigger chan struct{}) {
	if *timer != nil {
		(*timer).Stop()
	}
	now := f.clock.Now()

	earliest := now
	for _, req := range f.requests {
		// If this request already timed out, skip it altogether
		if req.hashes == nil {
			continue
		}
		if earliest > req.time {
			earliest = req.time
			if txFetchTimeout-time.Duration(now-earliest) < gatherSlack {
				break
			}
		}
	}
	*timer = f.clock.AfterFunc(txFetchTimeout-time.Duration(now-earliest), func() {
		trigger <- struct{}{}
	})
}

// scheduleFetches starts a batch of retrievals for all available idle peers.
func (f *FileDataFetcher) scheduleFetches(timer *mclock.Timer, timeout chan struct{}, whitelist map[string]struct{}) {
	// Gather the set of peers we want to retrieve from (default to all)
	actives := whitelist
	if actives == nil {
		actives = make(map[string]struct{})
		for peer := range f.announces {
			actives[peer] = struct{}{}
		}
	}
	if len(actives) == 0 {
		return
	}

	// For each active peer, try to schedule some fileData fetches
	idle := len(f.requests) == 0

	f.forEachPeer(actives, func(peer string) {
		if f.requests[peer] != nil {
			return // continue in the for-each
		}
		if len(f.announces[peer]) == 0 {
			return // continue in the for-each
		}
		var (
			hashes = make([]common.Hash, 0, maxFdRetrievals)
			bytes  uint64
		)
		f.forEachAnnounce(f.announces[peer], func(hash common.Hash, meta *fdMetadata) bool {
			// If the fileData is already fetching, skip to the next one
			if _, ok := f.fetching[hash]; ok {
				return true
			}
			// Mark the hash as fetching and stash away possible alternates
			f.fetching[hash] = peer

			if _, ok := f.alternates[hash]; ok {
				panic(fmt.Sprintf("alternate tracker already contains fetching item: %v", f.alternates[hash]))
			}
			f.alternates[hash] = f.announced[hash]
			delete(f.announced, hash)

			// Accumulate the hash and stop if the limit was reached
			hashes = append(hashes, hash)
			if len(hashes) >= maxFdRetrievals {
				return false // break in the for-each
			}
			if meta != nil { // Only set eth/68 and upwards
				bytes += uint64(meta.size)
				if bytes >= uint64(maxFdRetrievalSize) {
					return false
				}
			}
			return true // scheduled, try to add more
		})

		// If any hashes were allocated, request them from the peer
		if len(hashes) > 0 {
			f.requests[peer] = &fdRequest{hashes: hashes, time: f.clock.Now()}
			fdRequestOutMeter.Mark(int64(len(hashes)))

			go func(peer string, hashes []common.Hash) {
				// Try to fetch the fileData, but in case of a request
				// failure (e.g. peer disconnected), reschedule the hashes.
				log.Info("fetchFds-----", "peer", peer, "hash", hashes[0].String())
				if err := f.fetchFds(peer, hashes); err != nil {
					fdRequestFailMeter.Mark(int64(len(hashes)))
					f.Drop(peer)
				}
			}(peer, hashes)
		}
	})
	// If a new request was fired, schedule a timeout timer
	if idle && len(f.requests) > 0 {
		f.rescheduleTimeout(timer, timeout)
	}
}

// forEachPeer does a range loop over a map of peers in production, but during
// testing it does a deterministic sorted random to allow reproducing issues.
func (f *FileDataFetcher) forEachPeer(peers map[string]struct{}, do func(peer string)) {
	// If we're running production, use whatever Go's map gives us
	if f.rand == nil {
		for peer := range peers {
			do(peer)
		}
		return
	}
	// We're running the test suite, make iteration deterministic
	list := make([]string, 0, len(peers))
	for peer := range peers {
		list = append(list, peer)
	}
	sort.Strings(list)
	rotateStrings(list, f.rand.Intn(len(list)))
	for _, peer := range list {
		do(peer)
	}
}

// forEachAnnounce does a range loop over a map of announcements in production,
// but during testing it does a deterministic sorted random to allow reproducing
// issues.
func (f *FileDataFetcher) forEachAnnounce(announces map[common.Hash]*fdMetadata, do func(hash common.Hash, meta *fdMetadata) bool) {
	// If we're running production, use whatever Go's map gives us
	if f.rand == nil {
		for hash, meta := range announces {
			if !do(hash, meta) {
				return
			}
		}
		return
	}
	// We're running the test suite, make iteration deterministic
	list := make([]common.Hash, 0, len(announces))
	for hash := range announces {
		list = append(list, hash)
	}
	sortHashes(list)
	rotateHashes(list, f.rand.Intn(len(list)))
	for _, hash := range list {
		if !do(hash, announces[hash]) {
			return
		}
	}
}
