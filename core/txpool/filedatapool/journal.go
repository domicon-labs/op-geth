package filedatapool

import (
	"errors"
	"io"
	"io/fs"
	"os"

	"github.com/domicon-labs/op-geth/common"
	"github.com/domicon-labs/op-geth/core/types"
	"github.com/domicon-labs/op-geth/log"
	"github.com/domicon-labs/op-geth/rlp"
)

// errNoActiveJournal is returned if a fileData is attempted to be inserted
// into the journal, but no such file is currently open.
var errNoActiveJournal = errors.New("no active journal")

type devNull struct{}

func (*devNull) Write(p []byte) (n int, err error) { return len(p), nil }
func (*devNull) Close() error                      { return nil }

type journal struct {
	path string // Filesystem path to store the fileDatas at

	writer io.WriteCloser // Output stream to write new fileDatas into
}

// newFdJournal creates a new fileData journal to
func newFdJournal(path string) *journal {
	return &journal{
		path: path,
	}
}

// load parses a fileData journal dump from disk, loading its contents into
// the specified pool.
func (journal *journal) load(add func([]*types.FileData) []error) error {
	// Open the journal for loading any past fileDatas
	input, err := os.Open(journal.path)
	if errors.Is(err, fs.ErrNotExist) {
		// Skip the parsing if the journal file doesn't exist at all
		return nil
	}
	if err != nil {
		return err
	}
	defer input.Close()

	// Temporarily discard any journal additions (don't double add on load)
	journal.writer = new(devNull)
	defer func() { journal.writer = nil }()

	// Inject all fileDatas from the journal into the pool
	stream := rlp.NewStream(input, 0)
	total, dropped := 0, 0

	// Create a method to load a limited batch of fileDatas and bump the
	// appropriate progress counters. Then use this method to load all the
	// journaled fileDatas in small-ish batches.
	loadBatch := func(files types.FileDatas) {
		for _, err := range add(files) {
			if err != nil {
				log.Debug("Failed to add journaled fileData", "err", err)
				dropped++
			}
		}
	}
	var (
		failure error
		batch   types.FileDatas
	)
	for {
		// Parse the next FileData and terminate on error
		fd := new(types.FileData)
		if err = stream.Decode(fd); err != nil {
			if err != io.EOF {
				failure = err
			}
			if batch.Len() > 0 {
				loadBatch(batch)
			}
			break
		}
		// New fileData parsed, queue up for later, import if threshold is reached
		total++

		if batch = append(batch, fd); batch.Len() > 1024 {
			loadBatch(batch)
			batch = batch[:0]
		}
	}
	log.Info("Loaded local FileData journal", "FileDatas", total, "dropped", dropped)

	return failure
}

// insert adds the specified fileData to the local disk journal.
func (journal *journal) insert(fd *types.FileData) error {
	if journal.writer == nil {
		return errNoActiveJournal
	}
	if err := rlp.Encode(journal.writer, fd); err != nil {
		return err
	}
	return nil
}

// rotate regenerates the fileData journal based on the current contents of
// the fileData pool.
func (journal *journal) rotate(all map[common.Hash]*types.FileData) error {
	// Close the current journal (if any is open)
	if journal.writer != nil {
		if err := journal.writer.Close(); err != nil {
			return err
		}
		journal.writer = nil
	}
	// Generate a new journal with the contents of the current pool
	replacement, err := os.OpenFile(journal.path+".new", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	journaled := 0
	for _, fd := range all {
		if err = rlp.Encode(replacement, fd); err != nil {
			replacement.Close()
			return err
		}
		journaled += 1
	}
	replacement.Close()

	// Replace the live journal with the newly generated one
	if err = os.Rename(journal.path+".new", journal.path); err != nil {
		return err
	}
	sink, err := os.OpenFile(journal.path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	journal.writer = sink
	log.Info("Regenerated local fileData journal", "fileDatas", journaled, "accounts", len(all))

	return nil
}

// close flushes the fileData journal contents to disk and closes the file.
func (journal *journal) close() error {
	var err error

	if journal.writer != nil {
		err = journal.writer.Close()
		journal.writer = nil
	}
	return err
}
