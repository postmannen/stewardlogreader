package main

import (
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

// TODO:
// - Create verification checks that folders given in flags do exist.
// - If the "replies" folder don't exist, let the program create it.

var errIsDir = errors.New("is directory")

type server struct {
	fileState     *fileState
	configuration *configuration
}

func newServer() (*server, error) {
	configuration, err := newConfiguration()
	if err != nil {
		return &server{}, err
	}

	s := server{
		fileState:     newFileState(),
		configuration: configuration,
	}

	return &s, nil
}

type fileStatus int

const (
	statusLocked fileStatus = iota + 100
	statusCopied
)

type fileState struct {
	mu sync.Mutex
	m  map[string]fileInfo
}

func newFileState() *fileState {
	f := fileState{
		m: make(map[string]fileInfo),
	}

	return &f
}

type fileInfo struct {
	fileRealPath string
	fileName     string
	fileDir      string
	modTime      int64
	fileStatus   fileStatus
	isCopyError  bool
	isCopyReply  bool
	// Holds the filename, but with .copyerror or .copyreply suffix removed
	actualFileNameToCopy string
}

const copyReply = ".copyreply"
const copyError = ".copyerror"

// newFile will take the realPath takes a realpath to a file and returns a
// fileInfo structure  with the information like directory, filename, and
// last modified time split out in it's own fields, and return that.
// If an error happens, like that the verification that the file exists fail
// an empty fileInfo along side the error will be returned.
func newFileInfo(realPath string) (fileInfo, error) {
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
	}

	return fi, nil
}

type configuration struct {
	socketFullPath   string
	msgRepliesFolder string
	msgToNode        string

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
}

func newConfiguration() (*configuration, error) {
	c := configuration{}
	flag.StringVar(&c.socketFullPath, "socketFullPath", "", "the full path to the steward socket file")
	flag.StringVar(&c.msgRepliesFolder, "msgRepliesFolder", "", "the folder where steward will deliver reply messages for when the dst node have received the copy request")
	flag.StringVar(&c.msgToNode, "msgToNode", "", "the name of the (this) local steward instance where we inject messages on the socket")

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

func main() {
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

	err = s.startLogsWatcher(logWatcher)
	if err != nil {
		log.Printf("%v\n", err)
		os.Exit(1)
	}

	repliesWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer repliesWatcher.Close()

	err = s.startRepliesWatcher(repliesWatcher)
	if err != nil {
		log.Printf("%v\n", err)
		os.Exit(1)
	}

	err = s.getInitialFiles()
	if err != nil {
		log.Printf("%v\n", err)
		os.Exit(1)
	}

	go func() {
		// Ticker the interval for when to check for files.
		ticker := time.NewTicker(time.Second * (time.Duration(s.configuration.checkInterval)))
		defer ticker.Stop()

		for ; true; <-ticker.C {

			timeNow := time.Now().Unix()

			for k, v := range s.fileState.m {

				// If file already is in statusLocked ?
				if v.fileStatus == statusLocked {
					continue
				}

				age := timeNow - v.modTime
				if age > s.configuration.maxFileAge {
					log.Printf("info: file with age %v is older than maxAge, sending file to socket: %v\n", age, v.fileRealPath)

					// update the fileStatus filed of fileInfo, and also update the map element for the path.
					v.fileStatus = statusLocked
					s.fileState.mu.Lock()
					s.fileState.m[k] = v
					s.fileState.mu.Unlock()

					err = s.sendFile(v)
					if err != nil {
						log.Printf("%v\n", err)
						os.Exit(1)
					}

					continue
				}

			}

		}
	}()

	<-sigCh

}

func (s *server) startLogsWatcher(watcher *fsnotify.Watcher) error {

	// Start listening for events.
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				// log.Println("event:", event)

				if event.Op == notifyOp {

					fileInfo, err := newFileInfo(event.Name)
					if err != nil && err != errIsDir {
						log.Printf("error: failed to newFileInfo for path: %v\n", err)
						continue
					}

					// Add or update the information for the file in the map.
					s.fileState.mu.Lock()
					if _, exists := s.fileState.m[event.Name]; !exists {
						log.Println("info: found new file:", event.Name)
					}

					s.fileState.m[event.Name] = fileInfo
					s.fileState.mu.Unlock()

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

func (s *server) startRepliesWatcher(watcher *fsnotify.Watcher) error {

	// Start listening for events.
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				// Use Create for Linux
				// Use Chmod for mac

				if event.Op == notifyOp {
					fileInfoReplyFile, err := newFileInfo(event.Name)
					if err != nil {
						log.Printf("error: failed to newFileInfo for path: %v\n", err)
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
							log.Printf("error: failed to remove reply folder file: %v\n", err)
						}
					}

					err = os.Remove(copiedFileRealPath)
					if err != nil {
						log.Printf("error: failed to remove actual file: %v\n", err)
					}

					// Add or update the information for thefile in the map.
					s.fileState.mu.Lock()
					delete(s.fileState.m, copiedFileRealPath)
					s.fileState.mu.Unlock()

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
	err := watcher.Add(s.configuration.msgRepliesFolder)
	if err != nil {
		log.Fatal(err)
	}

	return nil
}

// sendFile is a wrapper function for the functions that
// will read File, send File and deletes the file.
func (s *server) sendFile(file fileInfo) error {

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
func (s *server) getInitialFiles() error {

	err := filepath.Walk(s.configuration.copySrcFolder,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			fmt.Println(path, info.Size())

			f, err := newFileInfo(path)
			if err != nil && err == errIsDir {
				return nil
			}
			if err != nil && err != errIsDir {
				return err
			}

			s.fileState.mu.Lock()
			s.fileState.m[path] = f
			s.fileState.mu.Unlock()

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
