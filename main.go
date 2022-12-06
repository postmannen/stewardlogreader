package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/fsnotify.v1"
	"gopkg.in/yaml.v3"
)

var errIsDir = errors.New("is directory")

// server holds the main structures used for the logreader.
type server struct {
	allFilesState *allFilesState
	configuration *configuration
}

// newServer will prepare and return a *server.
func newServer() (*server, error) {
	configuration, err := newConfiguration()
	if err != nil {
		return &server{}, err
	}

	s := server{
		allFilesState: newAllFilesState(),
		configuration: configuration,
	}

	return &s, nil
}

// fileState holds the variables needed for individial files to
// be put into lock state and also the context to be able to
// cancel the timeout if a file have become stale in the system.
type fileState struct {
	locked bool

	// Context and cancel function for an individial file used
	// to be able to know when to be able to change a map entry
	// for a file from locked to unlocked, so the message to
	// request a file copy can be reinitiated.
	cancel context.CancelFunc
	ctx    context.Context
}

// newFileState will prepare and return a *fileState.
func newFileState(ctx context.Context) *fileState {
	ctx, cancel := context.WithCancel(ctx)

	fs := fileState{
		locked: false,
		cancel: cancel,
		ctx:    ctx,
	}

	return &fs
}

type allFilesState struct {
	mu sync.Mutex
	// Map of all the files found, and the state if it is being
	// worked on by locking it.
	m map[string]fileInfo
	// Channel for receving values if we should free up a lock
	// defined for a file in the map.
	lockTimeoutCh chan keyValue
}

// nextUnlocked will return the next unlocked field in the map.
// Will also return a boolean value as 1 if an item was found, or
// 0 if no unlocked items were found.
func (f *allFilesState) nextUnlocked() (keyValue, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, v := range f.m {
		if v.fileState.locked {
			continue
		}

		kv := keyValue{k, v}
		return kv, true
	}

	return keyValue{}, false
}

// update will create/update the prived key/value in the map.
func (f *allFilesState) update(kv keyValue) {
	f.mu.Lock()
	f.m[kv.k] = kv.v
	f.mu.Unlock()
}

// exists will return true if a key element is found in the map.
func (f *allFilesState) exists(kv keyValue) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, exists := f.m[kv.k]

	return exists
}

// delete will delete a key value pair from the map.
func (f *allFilesState) delete(kv keyValue) {
	f.mu.Lock()
	delete(f.m, kv.k)
	f.mu.Unlock()

}

// cancelTimer will cancel the timer go routine belonging to a file
// that is used for checking for stale copy processes.
func (f *allFilesState) cancelTimer(kv keyValue) {
	f.mu.Lock()
	f.m[kv.k].fileState.cancel()
	f.mu.Unlock()

}

func newAllFilesState() *allFilesState {
	f := allFilesState{
		m:             make(map[string]fileInfo),
		lockTimeoutCh: make(chan keyValue),
	}

	return &f
}

// fileInfo holds the general information about a file.
type fileInfo struct {
	fileRealPath string
	fileName     string
	fileDir      string
	modTime      int64
	fileState    *fileState
	isCopyError  bool
	isCopyReply  bool
	// Holds the filename, but with .copyerror or .copyreply suffix removed
	actualFileNameToCopy string
}

const copyReply = ".copyreply"
const copyError = ".copyerror"

// newFileInfo will take the realPath takes a realpath to a file and returns a
// fileInfo structure  with the information like directory, filename, and
// last modified time split out in it's own fields, and return that.
// If an error happens, like that the verification that the file exists fail
// an empty fileInfo along side the error will be returned.
func newFileInfo(ctx context.Context, realPath string) (fileInfo, error) {
	fileName := filepath.Base(realPath)
	fileDir := filepath.Dir(realPath)

	inf, err := os.Stat(realPath)
	if err != nil {
		return fileInfo{}, fmt.Errorf("error: newFileInfo: os.Stat failed: %v", err)
	}

	if inf.IsDir() {
		return fileInfo{}, errIsDir
	}

	isCopyReply := strings.HasSuffix(realPath, copyReply)
	isCopyError := strings.HasSuffix(realPath, copyError)

	var actualFileNameToCopy string

	switch {
	case isCopyReply:
		actualFileNameToCopy = strings.TrimSuffix(fileName, copyReply)
	case isCopyError:
		actualFileNameToCopy = strings.TrimSuffix(fileName, copyError)
	}

	fi := fileInfo{
		fileRealPath:         realPath,
		fileName:             fileName,
		fileDir:              fileDir,
		modTime:              inf.ModTime().Unix(),
		isCopyReply:          isCopyReply,
		isCopyError:          isCopyError,
		actualFileNameToCopy: actualFileNameToCopy,
		//HERE!
		fileState: newFileState(ctx),
	}

	return fi, nil
}

// configuration holds all the general configuration options.
type configuration struct {
	socketFullPath   string
	msgRepliesFolder string
	msgToNode        string
	msgACKTimeout    int
	msgRetries       int

	copySrcFolder       string
	copyDstToNode       string
	copyDstFolder       string
	copyChunkSize       string
	copyMaxTransferTime string

	maxFileAge    int64
	checkInterval int
	prefixName    string
	prefixTimeNow bool

	deleteReplies bool

	maxCopyProcesses int
}

// newConfiguration will parse all the input flags, check if values
// have been set correctly, and return a *configuration.
func newConfiguration() (*configuration, error) {
	c := configuration{}
	flag.StringVar(&c.socketFullPath, "socketFullPath", "", "the full path to the steward socket file")
	flag.StringVar(&c.msgRepliesFolder, "msgRepliesFolder", "", "the folder where steward will deliver reply messages for when the dst node have received the copy request")
	flag.StringVar(&c.msgToNode, "msgToNode", "", "the name of the (this) local steward instance where we inject messages on the socket")
	// ---
	flag.IntVar(&c.msgACKTimeout, "msgACKTimeout", 5, "how long shall we wait for a steward message timeout in seconds")
	flag.IntVar(&c.msgRetries, "msgRetries", 1, "the number of retries we want to try sending a message before we give up")

	flag.StringVar(&c.copySrcFolder, "copySrcFolder", "", "the folder to watch")
	flag.StringVar(&c.copyDstToNode, "copyDstToNode", "", "the node to send the messages created to")
	flag.StringVar(&c.copyDstFolder, "copyDstFolder", "", "the folder at the destination to write files to.")
	flag.StringVar(&c.copyChunkSize, "copyChunkSize", "", "the chunk size to split files into while copying")
	flag.StringVar(&c.copyMaxTransferTime, "copyMaxTransferTime", "", "the max time a copy transfer operation are allowed to take in seconds")

	flag.Int64Var(&c.maxFileAge, "maxFileAge", 60, "how old a single file is allowed to be in seconds before it gets read and sent to the steward socket")
	flag.IntVar(&c.checkInterval, "checkInterval", 5, "the check interval in seconds")
	flag.StringVar(&c.prefixName, "prefixName", "", "name to be prefixed to the file name")
	flag.BoolVar(&c.prefixTimeNow, "prefixTimeNow", false, "set to true to prefix the filename with the time the file was piced up for copying")

	flag.BoolVar(&c.deleteReplies, "deleteReplies", true, "set to false to not delete the reply messages. Mainly used for debugging purposes")

	flag.IntVar(&c.maxCopyProcesses, "maxCopyProcesses", 5, "max copy processes to run simultaneously")

	flag.Parse()

	if c.socketFullPath == "" {
		return &configuration{}, fmt.Errorf("error: you need to specify the full path to the socket")
	}
	if c.msgRepliesFolder == "" {
		return &configuration{}, fmt.Errorf("error: you need to specify the msgRepliesFolder")
	}
	if c.msgToNode == "" {
		return &configuration{}, fmt.Errorf("error: you need to specify the msgToNode")
	}

	if c.copyDstToNode == "" {
		return &configuration{}, fmt.Errorf("error: you need to specify the copyDstToNode flag")
	}
	if c.copyDstFolder == "" {
		return &configuration{}, fmt.Errorf("error: you need to specify the copyDstFolder flag")
	}
	if c.copyChunkSize == "" {
		return &configuration{}, fmt.Errorf("error: you need to specify the copyChunkSize flag")
	}
	if c.copyMaxTransferTime == "" {
		return &configuration{}, fmt.Errorf("error: you need to specify the copyMaxTransferTime flag")
	}

	_, err := os.Stat(c.msgRepliesFolder)
	if err != nil {
		fmt.Printf("error: could not find replies folder, creating it\n")
		os.MkdirAll(c.msgRepliesFolder, 0755)
		if err != nil {
			return &configuration{}, fmt.Errorf("error: failed to create replies folder: %v", err)
		}
	}

	_, err = os.Stat(c.copySrcFolder)
	if err != nil {
		return &configuration{}, fmt.Errorf("error: the source folder to watch does not exist: %v", err)
	}

	return &c, nil
}

// startLockTimeoutReleaser will wait for lock timeout messages
// from the files that are currently active being handled, if
// a message is received we will set the locked state for that
// file to false, so the file will be scheduled for a retry at
// a later time.
func (s *server) startLockTimeoutReleaser(ctx context.Context) {
	for {
		select {
		case kv := <-s.allFilesState.lockTimeoutCh:
			// When a value is received on the channel here it means that the
			// ticker in the go routine who handles the timeout was reached,
			// and then the fileState goroutine have ended, so it is not need
			// to call cancel that go routine here. We just set the value for
			// the specific file to false so it will be retried later.

			kv.v.fileState.locked = false
			s.allFilesState.update(kv)
			log.Printf("info: received value on lockTimeoutCh, setting locked=false, and giving Cancel() to go routines for file: %v\n", kv.k)

		case <-ctx.Done():
			log.Printf("info: exiting startLockTimeoutReleaser\n")
			return
		}
	}
}

// kv represents a key value pair in the map
type keyValue struct {
	k string
	v fileInfo
}

// startLogsWatcher will start the watcher for checking for new files
// in the specified logs directory.
func (s *server) startLogsWatcher(ctx context.Context, watcher *fsnotify.Watcher) error {

	// Start listening for events.
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				if event.Op == notifyOp {

					fileInfo, err := newFileInfo(ctx, event.Name)
					if err != nil && err != errIsDir {
						// log.Printf("error: failed to newFileInfo for path: %v\n", err)
						continue
					}
					if err != nil && err == errIsDir {
						// log.Printf("error: failed to newFileInfo, is dir: %v\n", err)
						continue
					}

					// Add or update the information for the file in the map.
					exists := s.allFilesState.exists(keyValue{k: event.Name})

					if !exists {
						log.Println("info: found new file:", event.Name)
					}

					// Testing with canceling the timer. Since we've got notified of an update
					// that a file have been written to, we stop the go timer routine if one exist
					// before we do an update, and a new timer for that will file will be created
					// later.
					if exists {
						s.allFilesState.cancelTimer(keyValue{k: fileInfo.fileRealPath})
						log.Printf("info: logWatcher: updating file info in map, canceled timer for file: %v\n", fileInfo.fileRealPath)
					}

					s.allFilesState.update(keyValue{k: event.Name, v: fileInfo})

					// log.Printf("info: fileWatcher: updated map entry for file: fInfo contains: %v\n", fileInfo)

				}

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	// Add a path.
	err := watcher.Add(s.configuration.copySrcFolder)
	if err != nil {
		log.Fatal(err)
	}

	return nil
}

// startRepliesWatcher will start a watcher for the specified replies folder.
// When a reply is received we know that the copy was ok, and the belonging
// reply files and source files defined in the reply are deleted. The entry
// for the specific file are also removed from the map.
func (s *server) startRepliesWatcher(ctx context.Context, watcher *fsnotify.Watcher) error {

	// Remove any old content in the replies folder upon start.
	err := filepath.Walk(s.configuration.msgRepliesFolder,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if !os.FileInfo.IsDir(info) {
				os.Remove(path)
				fmt.Printf("info: deleted files from replies folder upon start: %v\n", path)
			}

			return nil
		})
	if err != nil {
		return err
	}

	// Start listening for events.
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				// The event types for the operating systems are different, we use :
				// - Create for Linux
				// - Chmod for mac

				// for copyReply files we want to delete the files if there is not a
				// corresponding real file entry in the map. The reason for this is
				// that this service might have been restarted and we receive messages
				// created by steward happened earlier but does no longer have a
				// registered state in the map, so we should just delete those, and let
				// the logreader redo them.

				if event.Op == notifyOp {
					fileInfoReplyFile, err := newFileInfo(ctx, event.Name)
					if err != nil {
						// log.Printf("error: failed to newFileInfo for path: %v\n", err)
						continue
					}

					if !fileInfoReplyFile.isCopyReply {
						// log.Printf("info: file was not a copyreply file\n")
						continue
					}

					// Get the realpath of the file in the logs folder
					copiedFileRealPath := filepath.Join(s.configuration.copySrcFolder, fileInfoReplyFile.actualFileNameToCopy)

					// Prepare the file path for eventual reply messages so we can check for them later.

					log.Printf("info: got copyreply message, copy went ok for: %v\n", fileInfoReplyFile.actualFileNameToCopy)

					if s.configuration.deleteReplies {
						err := os.Remove(fileInfoReplyFile.fileRealPath)
						if err != nil {
							log.Printf("error: failed to remove reply folder file: %v\n", err)
						}

						fname := filepath.Join(fileInfoReplyFile.fileDir, fileInfoReplyFile.actualFileNameToCopy)
						err = os.Remove(fname)
						if err != nil {
							log.Printf("error: failed to remove actual file: %v\n", err)
						}
					}

					err = os.Remove(copiedFileRealPath)
					if err != nil {
						log.Printf("error: failed to remove actual file: %v\n", err)
					}

					// Done with with file.
					// stop the timeout timer go routine for the specific file.
					s.allFilesState.cancelTimer(keyValue{k: copiedFileRealPath})
					log.Printf("got reply: canceled timer for file: %v\n", copiedFileRealPath)
					// delete the entry for the file int the map.
					s.allFilesState.delete(keyValue{k: copiedFileRealPath})

					// fmt.Printf("info: fileWatcher: deleted map entry for file: fInfo contains: %#v\n", actualFileRealPath)

					//}

				}

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	// Add a path.
	err = watcher.Add(s.configuration.msgRepliesFolder)
	if err != nil {
		log.Fatal(err)
	}

	return nil
}

// sendFile is a wrapper function for the functions that
// will read File, send File and deletes the file.
func (s *server) sendMessage(file fileInfo) error {

	// Append the actual filename to the directory specified in the msg template for
	// both source and destination.
	// Where source is  msg[0].MethodArgs[0],
	// and destination is, msg[0].MethodArgs[2]

	timeNow := strconv.Itoa(int(time.Now().Unix()))
	var prefix string
	switch {
	case s.configuration.prefixName == "" && s.configuration.prefixTimeNow:
		prefix = fmt.Sprintf("%s-", timeNow)
	case s.configuration.prefixName != "" && s.configuration.prefixTimeNow:
		prefix = fmt.Sprintf("%s-%s-", timeNow, s.configuration.prefixName)
	case s.configuration.prefixName != "" && !s.configuration.prefixTimeNow:
		prefix = fmt.Sprintf("%s-", s.configuration.prefixName)
	}

	m := Message{
		MethodArgs: make([]string, 5),
	}

	m.ToNode = Node(s.configuration.msgToNode)
	m.Method = "REQCopySrc"
	m.ACKTimeout = s.configuration.msgACKTimeout
	m.Retries = s.configuration.msgRetries

	m.MethodArgs[0] = filepath.Join(s.configuration.copySrcFolder, file.fileName)
	m.MethodArgs[1] = s.configuration.copyDstToNode
	m.MethodArgs[2] = filepath.Join(s.configuration.copyDstFolder, prefix+file.fileName)
	m.MethodArgs[3] = s.configuration.copyChunkSize
	m.MethodArgs[4] = s.configuration.copyMaxTransferTime

	// Make the correct real path for the .copied file, so we can check for this when we want to delete it.
	// We put the .copied.<...> file name in the "FileName" field of the message. This will instruct Steward
	// to create this file on the node it originated from when the Request is done. We can then use the existence of this file to know if a file copy was OK or NOT.
	m.Directory = s.configuration.msgRepliesFolder
	m.FileName = file.fileName

	msgs := []Message{m}

	err := messageToSocket(s.configuration.socketFullPath, msgs)
	if err != nil {
		return err
	}
	// log.Printf(" *** message for file %v have been put on socket\n", file.fileName)

	return nil
}

// getInitialFiles will look up all the files in the given folder,
func (s *server) getInitialFiles(ctx context.Context) error {

	err := filepath.Walk(s.configuration.copySrcFolder,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			fmt.Println(path, info.Size())

			f, err := newFileInfo(ctx, path)
			if err != nil && err == errIsDir {
				return nil
			}
			if err != nil && err != errIsDir {
				return err
			}

			s.allFilesState.update(keyValue{k: path, v: f})

			return nil
		})
	if err != nil {
		return err
	}

	return nil
}

// messageToSocket will write the message to the steward unix socket.
func messageToSocket(socketFullPath string, msg []Message) error {
	var socket net.Conn
	var err error

	for {
		socket, err = net.Dial("unix", socketFullPath)
		if err != nil && !strings.Contains(err.Error(), "connection refused") {
			return fmt.Errorf("error : could not open socket file for writing: %v", err)
		}
		if err != nil && strings.Contains(err.Error(), "connection refused") {
			fmt.Println("got connection refused, trying again")
			time.Sleep(time.Millisecond * 1000)
			continue
		}

		defer socket.Close()
		break
	}

	b, err := yaml.Marshal(msg)
	if err != nil {
		return fmt.Errorf("error: failed to marshal message: %v", err)
	}

	_, err = socket.Write(b)
	if err != nil {
		return fmt.Errorf("error: failed to write message to socket: %v", err)
	}

	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := newServer()
	if err != nil {
		log.Printf("%v\n", err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Start checking for copied files.
	// Create new watcher.
	logWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer logWatcher.Close()

	err = s.startLogsWatcher(ctx, logWatcher)
	if err != nil {
		log.Printf("%v\n", err)
		os.Exit(1)
	}

	repliesWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer repliesWatcher.Close()

	err = s.startRepliesWatcher(ctx, repliesWatcher)
	if err != nil {
		log.Printf("%v\n", err)
		os.Exit(1)
	}

	err = s.getInitialFiles(ctx)
	if err != nil {
		log.Printf("%v\n", err)
		os.Exit(1)
	}

	processFileCh := make(chan keyValue)
	maxCopyProcessesCh := make(chan struct{}, s.configuration.maxCopyProcesses)

	// Check file status at given interval, and start processing if file is old enough.
	go func() {
		// Ticker the interval for when to check for files.
		ticker := time.NewTicker(time.Second * (time.Duration(s.configuration.checkInterval)))
		defer ticker.Stop()

		for {
			select {
			// Loop over the map, and pick the first item that is not in statusLocked
			case <-ticker.C:

				// for k, v := range s.fileState.m {
				for {
					kv, ok := s.allFilesState.nextUnlocked()
					if !ok {
						fmt.Println("found no new unlocked items in the map")
						break
					}

					maxCopyProcessesCh <- struct{}{}

					log.Printf("info: got nextUnlocked file to be processed: %+v\n", kv.k)
					// Update the map element for the file with statusLocked.
					kv.v.fileState.locked = true
					s.allFilesState.update(kv)

					// Start up a go routine who will belong to the individual file,
					// and also be responsible for checking if the file is older than
					// max age. When the file is older than max age we send it to the
					// socket.
					go func() {
						log.Printf("info: start up check interval timer for file: %+v\n", kv.k)

						ticker2 := time.NewTicker(time.Second * (time.Duration(s.configuration.checkInterval)))
						defer ticker2.Stop()

						for {
							select {
							case <-ticker2.C:
								timeNow := time.Now().Unix()
								age := timeNow - kv.v.modTime

								if age > s.configuration.maxFileAge {
									log.Printf("info: file with age %v seconds is older than maxAge %v seconds, preparing to send file to socket: %v\n", age, s.configuration.maxFileAge, kv.v.fileRealPath)

									processFileCh <- kv
									<-maxCopyProcessesCh

									return

								}

								log.Printf("info: file not old enough, age %v seconds, maxAge set %v seconds, looping: %v\n", age, s.configuration.maxFileAge, kv.v.fileRealPath)

							case <-kv.v.fileState.ctx.Done():
								log.Printf("info: got <-ctx.Done checking for file age for file: %+v\n", kv.k)
								return
							}

						}
					}()

				}

			case <-ctx.Done():
				return
			}

		}
	}()

	// Read one value at a time from the channel, where each value represents
	// a file that are old enough to send a message to Steward to copy it.
	go func() {
		for {
			select {
			case kv := <-processFileCh:
				err = s.sendMessage(kv.v)
				if err != nil {
					log.Printf("%v\n", err)
					os.Exit(1)
				}

				// When the message is sent we want to create a function with a
				// timer on each file being processed, so if it takes to long
				// we can release the lock again, so the file will be retried
				// later.
				//
				// TODO: We should also have a context to be able to cancel the
				// function if the file is processed succesfully, so we also
				// need to put the cancel function inside the map value so we
				// can find the correct one and use it.

				go func(kv keyValue) {
					t, err := strconv.Atoi(s.configuration.copyMaxTransferTime)
					if err != nil {
						log.Printf("error: failed to convert copyMaxTransferTime to int: %v\n", err)
						os.Exit(1)
					}

					ticker := time.NewTicker(time.Second * time.Duration(t*s.configuration.msgRetries))

					select {
					case <-ticker.C:
						s.allFilesState.lockTimeoutCh <- kv
						log.Printf("info: sendt message and got lockTimeout, so we never got a reply message within the time, unlocking file to be reprocessed: %v\n", kv.k)

					// When a file is successfully copied, we should receive
					// a done signal here so we can return from this the go routine.
					case <-kv.v.fileState.ctx.Done():
						log.Printf("info: got <-ctx.Done on file: %v\n", kv.k)
						return
					}
				}(kv)

			case <-ctx.Done():
				return
			}

		}

	}()

	go func() {
		s.startLockTimeoutReleaser(ctx)
	}()

	<-sigCh

}
