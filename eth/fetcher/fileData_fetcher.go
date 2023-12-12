package fetcher

import (
	"errors"
	"fmt"
	mrand "math/rand"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/core/txpool/filedatapool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

var (
	// txArriveTimeout is the time allowance before an announced transaction is
	// explicitly requested.
	fdArriveTimeout = 500 * time.Millisecond
	// fdFetchTimeout is the maximum allotted time to return an explicitly
	// requested fileData.
	fdFetchTimeout = 5 * time.Second
)

var (

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

	fdFetcherWaitingPeers   = metrics.NewRegisteredGauge("eth/fetcher/filedata/waiting/peers", nil)
    fdFetcherWaitingHashes  = metrics.NewRegisteredGauge("eth/fetcher/filedata/waiting/hashes", nil)
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
	direct bool          // Whether this is a direct reply or a broadcast
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
	cleanup chan *fdDelivery
	drop    chan *fdDrop
	quit    chan struct{}

	// Stage 1: Waiting lists for newly discovered transactions that might be
	// broadcast without needing explicit request/reply round trips.
	waitlist  map[common.Hash]map[string]struct{}    // Transactions waiting for an potential broadcast
	waittime  map[common.Hash]mclock.AbsTime         // Timestamps when transactions were added to the waitlist


	// Stage 2: Queue of fileDatas that waiting to be allocated to some peer
	// to be retrieved directly.
	announced map[common.Hash]map[string]struct{}    // Set of download locations, grouped by transaction hash

	// Stage 3: Set of fileDatas currently being retrieved, some which may be
	// fulfilled and some rescheduled. Note, this step shares 'announces' from the
	// previous stage to avoid having to duplicate (need it for DoS checks).
	fetching   map[common.Hash]string              // Transaction set currently being retrieved
	requests   map[string]*fdRequest               // In-flight transaction retrievals
	alternates map[common.Hash]map[string]struct{} // In-flight transaction alternate origins if retrieval fails

	// Callbacks
	hasFd    func(common.Hash) bool             // Retrieves a tx from the local txpool
	addFds   func([]*types.FileData) []error // Insert a batch of transactions into local txpool
	fetchFds func(string, []common.Hash) error  // Retrieves a set of txs from a remote peer
	dropPeer func(string)                       // Drops a peer in case of announcement violation

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
		cleanup:     make(chan *fdDelivery),
		drop:        make(chan *fdDrop),
		quit:        make(chan struct{}),
		waitlist:    make(map[common.Hash]map[string]struct{}),
		waittime:    make(map[common.Hash]mclock.AbsTime),
		announced:   make(map[common.Hash]map[string]struct{}),
		fetching:    make(map[common.Hash]string),
		requests:    make(map[string]*fdRequest),
		alternates:  make(map[common.Hash]map[string]struct{}),
		hasFd:       hasFd,
		addFds:      addFds,
		fetchFds:    fetchFds,
		dropPeer:    dropPeer,
		clock:       clock,
		rand:        rand,
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

		log.Info("FileDataFetcher-----Enqueue----1")
		for _, err := range f.addFds(batch) {
			// Track a few interesting failure types
			switch {
			case err == nil: // Noop, but need to handle to not count these
				
			case errors.Is(err, filedatapool.ErrAlreadyKnown):
				duplicate++

			default:
				otherreject++
			}
			//added = append(added, batch[j].TxHash)
		}
		knownMeter.Mark(duplicate)
		otherRejectMeter.Mark(otherreject)

		// If 'other reject' is >25% of the deliveries in any batch, sleep a bit.
		if otherreject > 128/4 {
			time.Sleep(200 * time.Millisecond)
			log.Warn("Peer delivering stale transactions", "peer", peer, "rejected", otherreject)
		}
	}

	log.Info("FileDataFetcher-----Enqueue----2")
	if !direct {
		select {
		case f.cleanup <- &fdDelivery{origin: peer, hashes: added, direct: direct}:
			log.Info("FileDataFetcher-----Enqueue----3")
			return nil
		case <-f.quit:
			return errTerminated
		}
	}
	log.Info("FileDataFetcher-----Enqueue----4")
	return nil
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
		// case ann := <-f.notify:
		// 	// Drop part of the new announcements if there are too many accumulated.
		// 	// Note, we could but do not filter already known transactions here as
		// 	// the probability of something arriving between this call and the pre-
		// 	// filter outside is essentially zero.
		// 	used := len(f.waitslots[ann.origin]) + len(f.announces[ann.origin])
		// 	if used >= maxTxAnnounces {
		// 		// This can happen if a set of transactions are requested but not
		// 		// all fulfilled, so the remainder are rescheduled without the cap
		// 		// check. Should be fine as the limit is in the thousands and the
		// 		// request size in the hundreds.
		// 		txAnnounceDOSMeter.Mark(int64(len(ann.hashes)))
		// 		break
		// 	}
		// 	want := used + len(ann.hashes)
		// 	if want > maxTxAnnounces {
		// 		txAnnounceDOSMeter.Mark(int64(want - maxTxAnnounces))

		// 		ann.hashes = ann.hashes[:want-maxTxAnnounces]
		// 		ann.metas = ann.metas[:want-maxTxAnnounces]
		// 	}
		// 	// All is well, schedule the remainder of the transactions
		// 	idleWait := len(f.waittime) == 0
		// 	_, oldPeer := f.announces[ann.origin]

		// 	for i, hash := range ann.hashes {
		// 		// If the transaction is already downloading, add it to the list
		// 		// of possible alternates (in case the current retrieval fails) and
		// 		// also account it for the peer.
		// 		if f.alternates[hash] != nil {
		// 			f.alternates[hash][ann.origin] = struct{}{}

		// 			// Stage 2 and 3 share the set of origins per tx
		// 			if announces := f.announces[ann.origin]; announces != nil {
		// 				announces[hash] = ann.metas[i]
		// 			} else {
		// 				f.announces[ann.origin] = map[common.Hash]*txMetadata{hash: ann.metas[i]}
		// 			}
		// 			continue
		// 		}
		// 		// If the transaction is not downloading, but is already queued
		// 		// from a different peer, track it for the new peer too.
		// 		if f.announced[hash] != nil {
		// 			f.announced[hash][ann.origin] = struct{}{}

		// 			// Stage 2 and 3 share the set of origins per tx
		// 			if announces := f.announces[ann.origin]; announces != nil {
		// 				announces[hash] = ann.metas[i]
		// 			} else {
		// 				f.announces[ann.origin] = map[common.Hash]*txMetadata{hash: ann.metas[i]}
		// 			}
		// 			continue
		// 		}
		// 		// If the transaction is already known to the fetcher, but not
		// 		// yet downloading, add the peer as an alternate origin in the
		// 		// waiting list.
		// 		if f.waitlist[hash] != nil {
		// 			// Ignore double announcements from the same peer. This is
		// 			// especially important if metadata is also passed along to
		// 			// prevent malicious peers flip-flopping good/bad values.
		// 			if _, ok := f.waitlist[hash][ann.origin]; ok {
		// 				continue
		// 			}
		// 			f.waitlist[hash][ann.origin] = struct{}{}

		// 			if waitslots := f.waitslots[ann.origin]; waitslots != nil {
		// 				waitslots[hash] = ann.metas[i]
		// 			} else {
		// 				f.waitslots[ann.origin] = map[common.Hash]*txMetadata{hash: ann.metas[i]}
		// 			}
		// 			continue
		// 		}
		// 		// Transaction unknown to the fetcher, insert it into the waiting list
		// 		f.waitlist[hash] = map[string]struct{}{ann.origin: {}}
		// 		f.waittime[hash] = f.clock.Now()

		// 		if waitslots := f.waitslots[ann.origin]; waitslots != nil {
		// 			waitslots[hash] = ann.metas[i]
		// 		} else {
		// 			f.waitslots[ann.origin] = map[common.Hash]*txMetadata{hash: ann.metas[i]}
		// 		}
		// 	}
		// 	// If a new item was added to the waitlist, schedule it into the fetcher
		// 	if idleWait && len(f.waittime) > 0 {
		// 		f.rescheduleWait(waitTimer, waitTrigger)
		// 	}
		// 	// If this peer is new and announced something already queued, maybe
		// 	// request transactions from them
		// 	if !oldPeer && len(f.announces[ann.origin]) > 0 {
		// 		f.scheduleFetches(timeoutTimer, timeoutTrigger, map[string]struct{}{ann.origin: {}})
		// 	}

		case <-waitTrigger:
			// At least one fileData's waiting time ran out, push all expired
			// ones into the retrieval queues
			actives := make(map[string]struct{})
			for hash, instance := range f.waittime {
				if time.Duration(f.clock.Now()-instance)+txGatherSlack > fdArriveTimeout {
					// Transaction expired without propagation, schedule for retrieval
					if f.announced[hash] != nil {
						panic("announce tracker already contains waitlist item")
					}
					f.announced[hash] = f.waitlist[hash]
					for peer := range f.waitlist[hash] {
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
					// if len(f.announces[peer]) == 0 {
					// 	delete(f.announces, peer)
					// }
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
			log.Info("FileDataFetcher---loop---1","delivery.direct",delivery.direct)
			// In case of a direct delivery, also reschedule anything missing
			// from the original query
			if delivery.direct {
				// Mark the requesting successful (independent of individual status)
				fdRequestDoneMeter.Mark(int64(len(delivery.hashes)))

				// Make sure something was pending, nuke it
				req := f.requests[delivery.origin]
				if req == nil {
					log.Warn("Unexpected transaction delivery", "peer", delivery.origin)
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
							// delete(f.announces[delivery.origin], hash)
							// if len(f.announces[delivery.origin]) == 0 {
							// 	delete(f.announces, delivery.origin)
							// }
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
				log.Info("FileDataFetcher---loop---2","走到这了---")
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
	// if actives == nil {
	// 	actives = make(map[string]struct{})
	// 	for peer := range f.announces {
	// 		actives[peer] = struct{}{}
	// 	}
	// }
	if len(actives) == 0 {
		return
	}
	// For each active peer, try to schedule some transaction fetches
	idle := len(f.requests) == 0

	f.forEachPeer(actives, func(peer string) {
		if f.requests[peer] != nil {
			return // continue in the for-each
		}
		// if len(f.announces[peer]) == 0 {
		// 	return // continue in the for-each
		// }
		var (
			hashes = make([]common.Hash, 0, maxTxRetrievals)
			// bytes  uint64
		)
		// f.forEachAnnounce(f.announces[peer], func(hash common.Hash, meta *txMetadata) bool {
		// 	// If the transaction is already fetching, skip to the next one
		// 	if _, ok := f.fetching[hash]; ok {
		// 		return true
		// 	}
		// 	// Mark the hash as fetching and stash away possible alternates
		// 	f.fetching[hash] = peer

		// 	if _, ok := f.alternates[hash]; ok {
		// 		panic(fmt.Sprintf("alternate tracker already contains fetching item: %v", f.alternates[hash]))
		// 	}
		// 	f.alternates[hash] = f.announced[hash]
		// 	delete(f.announced, hash)

		// 	// Accumulate the hash and stop if the limit was reached
		// 	hashes = append(hashes, hash)
		// 	if len(hashes) >= maxTxRetrievals {
		// 		return false // break in the for-each
		// 	}
		// 	if meta != nil { // Only set eth/68 and upwards
		// 		bytes += uint64(meta.size)
		// 		if bytes >= maxTxRetrievalSize {
		// 			return false
		// 		}
		// 	}
		// 	return true // scheduled, try to add more
		// })
		// If any hashes were allocated, request them from the peer
		if len(hashes) > 0 {
			f.requests[peer] = &fdRequest{hashes: hashes, time: f.clock.Now()}
			fdRequestOutMeter.Mark(int64(len(hashes)))

			go func(peer string, hashes []common.Hash) {
				// Try to fetch the fileData, but in case of a request
				// failure (e.g. peer disconnected), reschedule the hashes.
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
