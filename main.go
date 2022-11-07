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
}

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

	fi := fileInfo{
		fileRealPath: realPath,
		fileName:     fileName,
		fileDir:      fileDir,
		modTime:      inf.ModTime().Unix(),
	}

	return fi, nil
}

type configuration struct {
	socketFullPath   string
	messageFullPath  string
	msgRepliesFolder string
	logFolder        string
	maxFileAge       int64
	checkInterval    int
	prefixName       string
	prefixTimeNow    bool
}

func newConfiguration() (*configuration, error) {
	c := configuration{}
	flag.StringVar(&c.socketFullPath, "socketFullPath", "", "the full path to the steward socket file")
	flag.StringVar(&c.messageFullPath, "messageFullPath", "./message-template.yaml", "the full path to the message to be used as the template for sending")
	flag.StringVar(&c.msgRepliesFolder, "msgRepliesFolder", "", "the folder where steward will deliver reply messages")
	flag.StringVar(&c.logFolder, "logFolder", "", "the log folder to watch")
	flag.Int64Var(&c.maxFileAge, "maxFileAge", 60, "how old a single file is allowed to be in seconds before it gets read and sent to the steward socket")
	flag.IntVar(&c.checkInterval, "checkInterval", 5, "the check interval in seconds")
	flag.StringVar(&c.prefixName, "prefixName", "", "name to be prefixed to the file name")
	flag.BoolVar(&c.prefixTimeNow, "prefixTimeNow", false, "set to true to prefix the filename with the time the file was piced up for copying")

	flag.Parse()

	if c.socketFullPath == "" {
		return &configuration{}, fmt.Errorf("error: you need to specify the full path to the socket")
	}

	_, err := os.Stat(c.messageFullPath)
	if err != nil {
		return &configuration{}, fmt.Errorf("error: could not find file: %v", err)
	}

	_, err = os.Stat(c.msgRepliesFolder)
	if err != nil {
		fmt.Printf("error: could not find replies folder, creating it\n")
		os.MkdirAll(c.msgRepliesFolder, 0755)
		if err != nil {
			return &configuration{}, fmt.Errorf("error: failed to create replies folder: %v", err)
		}
	}

	_, err = os.Stat(c.logFolder)
	if err != nil {
		return &configuration{}, fmt.Errorf("error: the log folder to watch does not exist: %v", err)
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
					fmt.Printf("info: file with age %v is older than maxAge, sending file to socket: %v\n", age, v.fileRealPath)

					// update the fileStatus filed of fileInfo, and also update the map element for the path.
					v.fileStatus = statusLocked
					s.fileState.mu.Lock()
					s.fileState.m[k] = v
					s.fileState.mu.Unlock()

					// Get the message template
					msg, err := readMessageTemplate(s.configuration.messageFullPath)
					if err != nil {
						log.Printf("%v\n", err)
						os.Exit(1)
					}

					// {
					// 	// TMP: Just print out the template for verification.
					// 	m, _ := yaml.Marshal(msg)
					// 	fmt.Printf("message template: %s\n", m)
					// }

					err = s.sendFile(msg, v)
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
					log.Println("info: got fsnotify CHMOD event:", event.Name)

					fileInfo, err := newFileInfo(event.Name)
					if err != nil && err != errIsDir {
						log.Printf("error: failed to newFileInfo for path: %v\n", err)
						continue
					}

					// Add or update the information for thefile in the map.
					s.fileState.mu.Lock()
					s.fileState.m[event.Name] = fileInfo
					s.fileState.mu.Unlock()

					fmt.Printf("info: fileWatcher: updated map entry for file: fInfo contains: %v\n", fileInfo)

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
	err := watcher.Add(s.configuration.logFolder)
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
					log.Println("info: startRepliesWatcher: got fsnotify CHMOD event:", event.Name)

					fileInfoReplyFile, err := newFileInfo(event.Name)
					if err != nil {
						log.Printf("error: failed to newFileInfo for path: %v\n", err)
						continue
					}

					// Get the path of the actual file in the logs folder
					actualFileRealPath := filepath.Join(s.configuration.logFolder, fileInfoReplyFile.fileName)

					_ = os.Remove(fileInfoReplyFile.fileRealPath)
					// if err != nil {
					// 	//log.Printf("error: failed to remove reply folder file: %v\n", err)
					// }

					_ = os.Remove(actualFileRealPath)
					// if err != nil {
					// 	log.Printf("error: failed to remove actual file: %v\n", err)
					// }

					// Add or update the information for thefile in the map.
					s.fileState.mu.Lock()
					delete(s.fileState.m, actualFileRealPath)
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
func (s *server) sendFile(msg []Message, file fileInfo) error {

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
	}

	// fmt.Printf(" * DEBUG: BEFORE APPEND: msg[0].MethodArgs[0]: %v\n", msg[0].MethodArgs[0])
	// fmt.Printf(" * DEBUG: BEFORE APPEND: msg[0].MethodArgs[2]: %v\n", msg[0].MethodArgs[2])
	msg[0].Method = "REQCopySrc"
	msg[0].MethodArgs[0] = filepath.Join(s.configuration.logFolder, file.fileName)
	msg[0].MethodArgs[2] = filepath.Join(msg[0].MethodArgs[2], prefix+file.fileName)
	// fmt.Printf(" * DEBUG: AFTER APPEND: msg[0].MethodArgs[0]: %v\n", msg[0].MethodArgs[0])
	// fmt.Printf(" * DEBUG: AFTER APPEND: msg[0].MethodArgs[2]: %v\n", msg[0].MethodArgs[2])

	// Make the correct real path for the .copied file, so we can check for this when we want to delete it.
	// We put the .copied.<...> file name in the "FileName" field of the message. This will instruct Steward
	// to create this file on the node it originated from when the Request is done. We can then use the existence of this file to know if a file copy was OK or NOT.
	msg[0].Directory = s.configuration.msgRepliesFolder
	msg[0].FileName = file.fileName

	err := messageToSocket(s.configuration.socketFullPath, msg)
	if err != nil {
		return err
	}
	fmt.Printf(" *** message for file %v have been put on socket\n", file.fileName)

	return nil
}

// getInitialFiles will look up all the files in the given folder,
func (s *server) getInitialFiles() error {

	err := filepath.Walk(s.configuration.logFolder,
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
	socket, err := net.Dial("unix", socketFullPath)
	if err != nil {
		return fmt.Errorf("error : could not open socket file for writing: %v", err)
	}
	defer socket.Close()

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
