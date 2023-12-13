package filedatapool

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

var (
	// ErrAlreadyKnown is returned if the fileData is already contained
	// within the pool.
	ErrAlreadyKnown = errors.New("already known")
)

var (
	evictionInterval = time.Minute // Time interval to check for evictable FileData

)

var (
	knownFdMeter       = metrics.NewRegisteredMeter("fileData/known", nil)
	invalidFdMeter     = metrics.NewRegisteredMeter("fileData/invalid", nil)
)

type Config struct {
	Journal   string           // Journal of local file to survive node restarts
	Locals    []common.Address // Addresses that should be treated by default as local

	Rejournal time.Duration    // Time interval to regenerate the local fileData journal
	// JournalRemote controls whether journaling includes remote transactions or not.
	// When true, all transactions loaded from the journal are treated as remote.
	JournalRemote bool
	Lifetime      time.Duration
}

var DefaultConfig = Config{
	Journal:  "fileData.rlp",
	Lifetime: 3 * 24 * time.Hour,
	Rejournal: time.Hour,
}

type BlockChain interface {
	// Config retrieves the chain's fork configuration.

	Config() *params.ChainConfig

	// CurrentBlock returns the current head of the chain.
	CurrentBlock() *types.Header

	// GetBlock retrieves a specific block, used during pool resets.
	GetBlock(hash common.Hash, number uint64) *types.Block

	// StateAt returns a state database for a given root hash (generally the head).
	StateAt(root common.Hash) (*state.StateDB, error)
}

type FilePool struct {
	config          Config
	chainconfig     *params.ChainConfig
	chain           BlockChain
	fileDataFeed    event.Feed
	mu              sync.RWMutex
	currentHead     atomic.Pointer[types.Header] // Current head of the blockchain
	currentState    *state.StateDB               // Current state in the blockchain head
	signer          types.Signer
	journal         *journal                // Journal of local fileData to back up to disk
	subs            event.SubscriptionScope // Subscription scope to unsubscribe all on shutdown
	all             *lookup
	collector       map[common.Hash]*types.FileData
	beats           map[common.Hash]time.Time // Last heartbeat from each known account
	//diskSaveCh      chan []common.Hash  // move fileData from memory pool to disk 
	reqResetCh      chan *fppoolResetRequest
	reorgDoneCh     chan chan struct{}
	reorgShutdownCh chan struct{}  // requests shutdown of scheduleReorgLoop
	wg              sync.WaitGroup // tracks loop, scheduleReorgLoop
	initDoneCh      chan struct{}  // is closed once the pool is initialized (for tests)
}

func New(config Config, chain BlockChain) *FilePool {
	fp := &FilePool{
		config:          config,
		chain:			 chain,		
		chainconfig:     chain.Config(),
		signer:          types.LatestSigner(chain.Config()),
		all:             newLookup(),
		collector:       make(map[common.Hash]*types.FileData),
		beats:           make(map[common.Hash]time.Time),
		reqResetCh:      make(chan *fppoolResetRequest),
		//diskSaveCh:      make(chan []common.Hash),
		reorgDoneCh:     make(chan chan struct{}),
		reorgShutdownCh: make(chan struct{}),
		initDoneCh:      make(chan struct{}),
	}

	if (config.JournalRemote) && config.Journal != "" {
		fp.journal = newFdJournal(config.Journal)
	}

	fp.Init(chain.CurrentBlock())
	return fp
}


// func (fp *FilePool) ReceiptCh() chan []*types.Receipt{
// 	return fp.diskSaveCh
// }


func (fp *FilePool) Init(head *types.Header) error {
	// Initialize the state with head block, or fallback to empty one in
	// case the head state is not available(might occur when node is not
	// fully synced).
	statedb, err := fp.chain.StateAt(head.Root)
	if err != nil {
		statedb, err = fp.chain.StateAt(types.EmptyRootHash)
	}
	if err != nil {
		return err
	}
	fp.currentHead.Store(head)
	fp.currentState = statedb

	// // Start the reorg loop early, so it can handle requests generated during
	// // journal loading.
	// fp.wg.Add(1)
	// go fp.scheduleReorgLoop()

	// If local fileData and journaling is enabled, load from disk
	if fp.journal != nil {
		add := fp.addLocals
		if fp.config.JournalRemote {
			add = fp.addRemotesSync // Use sync version to match pool.AddLocals
		}
		if err := fp.journal.load(add); err != nil {
			log.Warn("Failed to load transaction journal", "err", err)
		}
		if err := fp.journal.rotate(fp.toJournal()); err != nil {
			log.Warn("Failed to rotate transaction journal", "err", err)
		}
	}
	fp.wg.Add(1)
	go fp.loop()
	return nil
}

func (fp *FilePool) loop() {
	defer fp.wg.Done()

	var (
		// Start the stats reporting and fileData eviction tickers
		evict   = time.NewTicker(evictionInterval)
		journal = time.NewTicker(fp.config.Rejournal)
	)
	defer evict.Stop()
	defer journal.Stop()

	// Notify tests that the init phase is done
	close(fp.initDoneCh)
	for {
		select {
		// Handle pool shutdown
		case <-fp.reorgShutdownCh:
			return

		// // new transaction is writed on chain
		// case receipts := <-fp.diskSaveCh:

		// 	fp.mu.Lock()
		// 	for _,receipt := range receipts {
		// 		txHash := receipt.TxHash
		// 		err := fp.saveFileDataToDisk(txHash)
		// 		if err == nil {
		// 			fp.removeFileData(txHash)
		// 		}
		// 	}			
		// 	fp.mu.Unlock()

		// Handle inactive txHash fileData eviction
		case <-evict.C:
			fp.mu.Lock()
			for txHash := range fp.collector {
				// Any non-locals old enough should be removed
				if time.Since(fp.beats[txHash]) > fp.config.Lifetime {
					for txHash := range fp.collector {
						fp.removeFileData(txHash)
					}
				}
			}
			fp.mu.Unlock()

		// Handle local fileData journal rotation
		case <-journal.C:
			if fp.journal != nil {
				fp.mu.Lock()
				if err := fp.journal.rotate(fp.toJournal()); err != nil {
					log.Warn("Failed to rotate local fileData journal", "err", err)
				}
				fp.mu.Unlock()
			}
		}
	}
}

// // scheduleReorgLoop schedules runs of reset and promoteExecutables. Code above should not
// // call those methods directly, but request them being run using requestReset and
// // requestPromoteExecutables instead.
// func (fp *FilePool) scheduleReorgLoop() {
// 	defer fp.wg.Done()

// 	var (
// 		curDone       chan struct{} // non-nil while runReorg is active
// 		nextDone      = make(chan struct{})
// 		launchNextRun bool
// 		reset         *fppoolResetRequest
// 	)
// 	for {
// 		// Launch next background reorg if needed
// 		if curDone == nil && launchNextRun {
// 			// Run the background reorg and announcements
// 			go fp.runReorg(nextDone, reset)

// 			// Prepare everything for the next round of reorg
// 			curDone, nextDone = nextDone, make(chan struct{})
// 			launchNextRun = false
// 			reset = nil
// 		}

// 		select {
// 		case req := <-fp.reqResetCh:
// 			// Reset request: update head if request is already pending.
// 			if reset == nil {
// 				reset = req
// 			} else {
// 				reset.newHead = req.newHead
// 			}
// 			launchNextRun = true
// 			fp.reorgDoneCh <- nextDone
// 		case <-curDone:
// 			curDone = nil

// 		case <-fp.reorgShutdownCh:
// 			// Wait for current run to finish.
// 			if curDone != nil {
// 				<-curDone
// 			}
// 			close(nextDone)
// 			return
// 		}
// 	}
// }

// // runReorg runs reset and promoteExecutables on behalf of scheduleReorgLoop.
// func (fp *FilePool) runReorg(done chan struct{}, reset *fppoolResetRequest) {
// 	defer func(t0 time.Time) {
// 		reorgDurationTimer.Update(time.Since(t0))
// 	}(time.Now())
// 	defer close(done)

// 	fp.mu.Lock()
// 	if reset != nil {
// 		// Reset from the old head to the new, rescheduling any reorged transactions
// 		fp.reset(reset.oldHead, reset.newHead)
// 	}
// 	fp.mu.Unlock()

// }

// // reset retrieves the current state of the blockchain and ensures the content
// // of the transaction pool is valid with regard to the chain state.
// func (fp *FilePool) reset(oldHead, newHead *types.Header) {
// 	// If we're reorging an old state, reinject all dropped transactions
// 	//var reinject types.FileDatas

// 	if oldHead != nil && oldHead.Hash() != newHead.ParentHash {
// 		// If the reorg is too deep, avoid doing it (will happen during fast sync)
// 		oldNum := oldHead.Number.Uint64()
// 		newNum := newHead.Number.Uint64()
// 		if depth := uint64(math.Abs(float64(oldNum) - float64(newNum))); depth > 64 {
// 			log.Debug("Skipping deep transaction reorg", "depth", depth)
// 		} else {
// 			// Reorg seems shallow enough to pull in all transactions into memory
// 			var (
// 				rem = fp.chain.GetBlock(oldHead.Hash(), oldHead.Number.Uint64())
// 				add = fp.chain.GetBlock(newHead.Hash(), newHead.Number.Uint64())
// 			)
// 			if rem == nil {
// 				// This can happen if a setHead is performed, where we simply discard the old
// 				// head from the chain.
// 				// If that is the case, we don't have the lost transactions anymore, and
// 				// there's nothing to add
// 				if newNum >= oldNum {
// 					// If we reorged to a same or higher number, then it's not a case of setHead
// 					log.Warn("Transaction pool reset with missing old head",
// 						"old", oldHead.Hash(), "oldnum", oldNum, "new", newHead.Hash(), "newnum", newNum)
// 					return
// 				}
// 				// If the reorg ended up on a lower number, it's indicative of setHead being the cause
// 				log.Debug("Skipping transaction reset caused by setHead",
// 					"old", oldHead.Hash(), "oldnum", oldNum, "new", newHead.Hash(), "newnum", newNum)
// 				// We still need to update the current state s.th. the lost transactions can be readded by the user
// 			} else {
// 				if add == nil {
// 					// if the new head is nil, it means that something happened between
// 					// the firing of newhead-event and _now_: most likely a
// 					// reorg caused by sync-reversion or explicit sethead back to an
// 					// earlier block.
// 					log.Warn("Transaction pool reset with missing new head", "number", newHead.Number, "hash", newHead.Hash())
// 					return
// 				}
// 				var discarded, included types.Transactions
// 				for rem.NumberU64() > add.NumberU64() {
// 					discarded = append(discarded, rem.Transactions()...)
// 					if rem = fp.chain.GetBlock(rem.ParentHash(), rem.NumberU64()-1); rem == nil {
// 						log.Error("Unrooted old chain seen by tx pool", "block", oldHead.Number, "hash", oldHead.Hash())
// 						return
// 					}
// 				}
// 				for add.NumberU64() > rem.NumberU64() {
// 					included = append(included, add.Transactions()...)
// 					if add = fp.chain.GetBlock(add.ParentHash(), add.NumberU64()-1); add == nil {
// 						log.Error("Unrooted new chain seen by tx pool", "block", newHead.Number, "hash", newHead.Hash())
// 						return
// 					}
// 				}
// 				for rem.Hash() != add.Hash() {
// 					discarded = append(discarded, rem.Transactions()...)
// 					if rem = fp.chain.GetBlock(rem.ParentHash(), rem.NumberU64()-1); rem == nil {
// 						log.Error("Unrooted old chain seen by tx pool", "block", oldHead.Number, "hash", oldHead.Hash())
// 						return
// 					}
// 					included = append(included, add.Transactions()...)
// 					if add = fp.chain.GetBlock(add.ParentHash(), add.NumberU64()-1); add == nil {
// 						log.Error("Unrooted new chain seen by tx pool", "block", newHead.Number, "hash", newHead.Hash())
// 						return
// 					}
// 				}

// 				//modify by echo
// 				// lost := make([]*types.Transaction,0)
// 				// for _, tx := range types.TxDifference(discarded, included) {
// 				// 	if fp.Filter(tx) {
// 				// 		lost = append(lost, tx)
// 				// 	}
// 				// }

// 				//load from db
// 				// for _,tx := range lost {
// 				// 	fp.currentState.GetState()
// 				// }

// 				// reinject = lost
// 			}
// 		}
// 	}
// 	// Initialize the internal state to the current head
// 	if newHead == nil {
// 		newHead = fp.chain.CurrentBlock() // Special case during testing
// 	}
// 	statedb, err := fp.chain.StateAt(newHead.Root)
// 	if err != nil {
// 		log.Error("Failed to reset txpool state", "err", err)
// 		return
// 	}
// 	fp.currentHead.Store(newHead)
// 	fp.currentState = statedb

// 	// // Inject any transactions discarded due to reorgs
// 	// log.Debug("Reinjecting stale transactions", "count", len(reinject))
// 	// core.SenderCacher.Recover(pool.signer, reinject)
// 	// pool.addTxsLocked(reinject, false)
// }

// SubscribeFileDatas registers a subscription for new FileData events,
// supporting feeding only newly seen or also resurrected FileData.
func (fp *FilePool) SubscribenFileDatas(ch chan<- core.NewFileDataEvent) event.Subscription {
	// The legacy pool has a very messed up internal shuffling, so it's kind of
	// hard to separate newly discovered fileData from resurrected ones. This
	// is because the new fileDatas are added to , resurrected ones too and
	// reorgs run lazily, so separating the two would need a marker.
	return fp.fileDataFeed.Subscribe(ch)
}

func (fp *FilePool) SaveFileDataToDisk(hash common.Hash) error {
	fileData, ok := fp.all.collector[hash]
	if !ok {
		return errors.New("file pool dont have fileData")
	}

	diskDb := fp.currentState.Database().DiskDB()

	data, err := rlp.EncodeToBytes(fileData)
	if err != nil {
		return err
	}
	diskDb.Put(hash[:], data)

	fp.removeFileData(hash)
	return nil
}

func (fp *FilePool) removeFileData(hash common.Hash) error {
	fd := fp.all.Get(hash)
	if fd == nil {
		return errors.New("fileData with that fd hash not exist")
	}
	delete(fp.beats, hash)
	fp.all.Remove(hash)
	delete(fp.collector, hash)
	return nil
}

// cached with the given hash.
func (fp *FilePool) Has(hash common.Hash) bool{
	fd := fp.get(hash)
	return fd != nil
}


// Get retrieves the fileData from local fileDataPool with given
// tx hash.
func (fp *FilePool) Get(hash common.Hash) *types.FileData{
	fd := fp.get(hash)
	if fd == nil {
		return nil
	}
	return fd
}


// get returns a transaction if it is contained in the pool and nil otherwise.
func (fp *FilePool) get(hash common.Hash) *types.FileData {
	return fp.all.Get(hash)
}


// addRemotesSync is like addRemotes, but waits for pool reorganization. Tests use this method.
func (fp *FilePool) addRemotesSync(fds []*types.FileData) []error {
	return fp.Add(fds, false, true)
}

// toJournal retrieves all FileData that should be included in the journal,
// grouped by origin account and sorted by nonce.
// The returned FileData set is a copy and can be freely modified by calling code.
func (fp *FilePool) toJournal() map[common.Hash]*types.FileData {
	fds := make(map[common.Hash]*types.FileData)
	for hash, fd := range fp.collector {
		fds[hash] = fd
	}
	return fds
}

// addLocals enqueues a batch of FileData into the pool if they are valid, marking the
// senders as local ones, ensuring they go around the local pricing constraints.
//
// This method is used to add FileData from the RPC API and performs synchronous pool
// reorganization and event propagation.
func (fp *FilePool) addLocals(fds []*types.FileData) []error {
	return fp.Add(fds, true, true)
}

// Add enqueues a batch of FileData into the pool if they are valid. Depending
// on the local flag, full pricing constraints will or will not be applied.
//
// If sync is set, the method will block until all internal maintenance related
// to the add is finished. Only use this during tests for determinism!
func (fp *FilePool) Add(fds []*types.FileData, local, sync bool) []error {
	// Filter out known ones without obtaining the pool lock or recovering signatures
	var (
		errs = make([]error, len(fds))
		news = make([]*types.FileData, 0, len(fds))
	)
	for i, fd := range fds {
		// If the transaction is known, pre-set the error slot
		if fp.all.Get(fd.TxHash) != nil {
			errs[i] = ErrAlreadyKnown
			knownFdMeter.Mark(1)
			continue
		}

		txHash := fd.TxHash.String()

		log.Info("FilePool----Add","txHash",txHash)
		// Exclude transactions with basic errors, e.g invalid signatures and
		// insufficient intrinsic gas as soon as possible and cache senders
		// in transactions before obtaining lock
		if err := fp.validateFileDataSignature(fd, local); err != nil {
			errs[i] = err
			invalidFdMeter.Mark(1)
			continue
		}
		// Accumulate all unknown transactions for deeper processing
		news = append(news, fd)
	}
	if len(news) == 0 {
		return errs
	}

	// Process all the new transaction and merge any errors into the original slice
	fp.mu.Lock()
	newErrs := fp.addTxsLocked(news, local)
	fp.mu.Unlock()

	var nilSlot = 0
	var final = make([]*types.FileData, 0)
	for index, err := range newErrs {
		if err == nil {
			final = append(final,news[index])
		}
		for errs[nilSlot] != nil {
			nilSlot++
		}
		errs[nilSlot] = err
		nilSlot++
	}

	if len(final) != 0 {
		fp.fileDataFeed.Send(core.NewFileDataEvent{Fileds: final})
	}
	return errs
}

// addTxsLocked attempts to queue a batch of transactions if they are valid.
// The transaction pool lock must be held.
func (fp *FilePool) addTxsLocked(txs []*types.FileData, local bool) []error {
	errs := make([]error, len(txs))
	for i, tx := range txs {
		_, err := fp.add(tx, local)
		errs[i] = err
	}

	return errs
}

// add validates a transaction and inserts it into the non-executable queue for later
// pending promotion and execution. If the transaction is a replacement for an already
// pending or queued one, it overwrites the previous transaction if its price is higher.
//
// If a newly added transaction is marked as local, its sending account will be
// added to the allowlist, preventing any associated transaction from being dropped
// out of the pool due to pricing constraints.
func (fp *FilePool) add(fd *types.FileData, local bool) (replaced bool, err error) {
	// If the transaction is already known, discard it
	hash := fd.TxHash
	if fp.all.Get(hash) != nil {
		log.Trace("Discarding already known transaction", "hash", hash)
		knownFdMeter.Mark(1)
		return false, ErrAlreadyKnown
	}

	fp.journalTx(hash, fd)
	fp.all.Add(fd)
	fp.beats[hash] = time.Now()

	log.Trace("Pooled new future transaction", "hash", hash)
	return replaced, nil
}

// journalTx adds the specified transaction to the local disk journal if it is
// deemed to have been sent from a local account.
func (fp *FilePool) journalTx(txHash common.Hash, fd *types.FileData) {
	// Only journal if it's enabled and the transaction is local
	_, flag := fp.collector[txHash]
	if fp.journal == nil || (!fp.config.JournalRemote && !flag) {
		return
	}
	if err := fp.journal.insert(fd); err != nil {
		log.Warn("Failed to journal local transaction", "err", err)
	}
}

// validateTxBasics checks whether a transaction is valid according to the consensus
// rules, but does not check state-dependent validation such as sufficient balance.
// This check is meant as an early check which only needs to be performed once,
// and does not require the pool mutex to be held.
func (fp *FilePool) validateFileDataSignature(fd *types.FileData, local bool) error {

	//fp.signer.SignatureValues()


	return nil
}

// Close terminates the transaction pool.
func (fp *FilePool) Close() error {
	// Terminate the pool reorger and return
	close(fp.reorgShutdownCh)
	fp.wg.Wait()

	fp.subs.Close()

	if fp.journal != nil {
		fp.journal.close()
	}
	log.Info("FilePool pool stopped")
	return nil
}

type lookup struct {
	lock      sync.RWMutex
	collector map[common.Hash]*types.FileData
}

// newLookup returns a new lookup structure.
func newLookup() *lookup {
	return &lookup{
		collector: make(map[common.Hash]*types.FileData),
	}
}

// Range calls f on each key and value present in the map. The callback passed
// should return the indicator whether the iteration needs to be continued.
// Callers need to specify which set (or both) to be iterated.
func (t *lookup) Range(f func(hash common.Hash, fd *types.FileData) bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	for key, value := range t.collector {
		if !f(key, value) {
			return
		}
	}
}

// Get returns a transaction if it exists in the lookup, or nil if not found.
func (t *lookup) Get(hash common.Hash) *types.FileData {
	t.lock.RLock()
	defer t.lock.RUnlock()

	if tx := t.collector[hash]; tx != nil {
		return tx
	}
	return nil
}

// Count returns the current number of FileData in the lookup.
func (t *lookup) Count() int {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return len(t.collector)
}

// Add adds a transaction to the lookup.
func (t *lookup) Add(fd *types.FileData) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.collector[fd.TxHash] = fd
}

// Remove removes a transaction from the lookup.
func (t *lookup) Remove(hash common.Hash) {
	t.lock.Lock()
	defer t.lock.Unlock()

	delete(t.collector, hash)
}

type fppoolResetRequest struct {
	oldHead, newHead *types.Header
}
