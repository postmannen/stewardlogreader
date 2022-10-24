package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/fsnotify.v1"
	"gopkg.in/yaml.v3"
)

// TODO:
//  - If there is a lock file for a file, skip working on the file.

func main() {
	socketFullPath := flag.String("socketFullPath", "", "the full path to the steward socket file")
	messageFullPath := flag.String("messageFullPath", "./message.yaml", "the full path to the message to be used as the template for sending")
	logFolder := flag.String("logFolder", "", "the log folder to watch")
	maxFileAge := flag.Int("maxFileAge", 60, "how old a single file is allowed to be in seconds before it gets read and sent to the steward socket")
	checkInterval := flag.Int("checkInterval", 5, "the check interval in seconds")
	flag.Parse()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	if *socketFullPath == "" {
		log.Printf("error: you need to specify the full path to the socket\n")
		return
	}

	ticker := time.NewTicker(time.Second * (time.Duration(*checkInterval)))
	defer ticker.Stop()

	// Start checking for copied files.
	// Create new watcher.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	err = deleteCopiedFilesWatcher(watcher, *logFolder)
	if err != nil {
		log.Printf("%v\n", err)
		os.Exit(1)
	}

	go func() {
		for ; true; <-ticker.C {

			files, err := getFilesSorted(*logFolder)
			if err != nil {
				log.Printf("%v\n", err)
				os.Exit(1)
			}

			for i := range files {
				fmt.Println()

				age, err := fileIsHowOld(files[i].fileRealPath)
				if err != nil {
					log.Printf("%v\n", err)
					os.Exit(1)
				}

				if age > *maxFileAge {
					// TODO: Read the content, and create message with data, and send it here.
					fmt.Printf(" * age of file %v, is %v\n", files[i].fileRealPath, age)
					fmt.Printf(" * file is older than maxAge, sending file to socket: %v\n", files[i].fileRealPath)

					// HERE!!!
					// Get the message template
					msg, err := readMessageTemplate(*messageFullPath)
					if err != nil {
						log.Printf("%v\n", err)
						os.Exit(1)
					}

					{
						// TMP: Just print out the template for verification.
						m, _ := yaml.Marshal(msg)
						fmt.Printf("message template: %s\n", m)
					}
					// !!!

					err = sendFile(msg, files[i], *socketFullPath)
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

func deleteCopiedFilesWatcher(watcher *fsnotify.Watcher, logFolder string) error {

	// Start listening for events.
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Println("event:", event)
				if event.Op == fsnotify.Create {
					log.Println("************ WRITE file:", event.Name)

					fileName := filepath.Base(event.Name)
					fileDir := filepath.Dir(event.Name)
					realPath := event.Name
					{
						// Check if the file is a .copied, or that it got a .copied file,
						// and if so, delete both .copied and .lock and realPath files.
						switch {
						case strings.HasPrefix(fileName, ".copied."):
							fmt.Println("FOUND .COPIED FILE")

							// Also get the name of the actual log file without the .copied.
							actualLogFileName := strings.TrimPrefix(fileName, ".copied.")
							actualLogFileFullPath := filepath.Join(fileDir, actualLogFileName)

							// First delete the actual log file.
							err := os.Remove(actualLogFileFullPath)
							if err != nil {
								log.Printf("error: failed to remove the file actual log file: %v\n", err)
							}

							// Then delete the the .copied.<...> file.
							err = os.Remove(realPath)
							if err != nil {
								log.Printf("error: failed to remove the file .copied file: %v\n", err)
							}

							// Delete any .lock. files
							lockFileFullPath := filepath.Join(fileDir, ".lock."+actualLogFileName)
							err = os.Remove(lockFileFullPath)
							if err != nil {
								log.Printf("error: failed to remove the .lock file: %v\n", err)
							}
							fmt.Printf("successfully delete lock file: %v\n", lockFileFullPath)

						}
					}
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
	err := watcher.Add(logFolder)
	if err != nil {
		log.Fatal(err)
	}

	return nil
}

// sendFile is a wrapper function for the functions that
// will read File, send File and deletes the file.
func sendFile(msg []Message, file fileInfo, socketFullPath string) error {

	// Create a .lock.<..> file
	//fileDir := filepath.Dir(file.fullPath)
	//lockFileRealPath := filepath.Join(fileDir, ".lock."+file.fileName)
	fhLock, err := os.Create(file.lockFileRealPath)
	if err != nil {
		return fmt.Errorf("error: failed to create lock file: %v", err)
	}
	fhLock.Close()

	// Append the actual filename to the directory specified in the msg template for
	// both source and destination.
	// Where source is  msg[0].MethodArgs[0],
	// and destination is, msg[0].MethodArgs[2]
	fmt.Printf(" * DEBUG: BEFORE APPEND: msg[0].MethodArgs[0]: %v\n", msg[0].MethodArgs[0])
	fmt.Printf(" * DEBUG: BEFORE APPEND: msg[0].MethodArgs[2]: %v\n", msg[0].MethodArgs[2])
	msg[0].MethodArgs[0] = filepath.Join(msg[0].MethodArgs[0], file.fileName)
	msg[0].MethodArgs[2] = filepath.Join(msg[0].MethodArgs[2], file.fileName)
	fmt.Printf(" * DEBUG: AFTER APPEND: msg[0].MethodArgs[0]: %v\n", msg[0].MethodArgs[0])
	fmt.Printf(" * DEBUG: AFTER APPEND: msg[0].MethodArgs[2]: %v\n", msg[0].MethodArgs[2])

	// Make the correct real path for the .copied file, so we can check for this when we want to delete it.
	// We put the .copied.<...> file name in the "FileName" field of the message. This will instruct Steward
	// to create this file on the node it originated from when the Request is done. We can then use the existence of this file to know if a file copy was OK or NOT.
	msg[0].FileName = filepath.Join(".copied." + file.fileName)
	fmt.Printf(" * DEBUG: Before putting file on socket, fileName = %v\n", file.fileName)
	err = messageToSocket(socketFullPath, msg)
	if err != nil {
		return err
	}
	fmt.Printf(" *** message for file %v have been put on socket\n", file.fileName)

	return nil
}

// fileIsHowOld will return how old a file is in minutes.
func fileIsHowOld(fileName string) (int, error) {
	fi, err := os.Stat(fileName)
	if err != nil {
		return 0, fmt.Errorf("error: fileIshowOld os.Stat failed: %v", err)
	}

	modTime := (time.Now().Unix() - fi.ModTime().Unix())
	return int(modTime), nil
}

type fileInfo struct {
	fileRealPath       string
	fileName           string
	fileDir            string
	lockFileRealPath   string
	copiedFileRealPath string
	nameWithoutDate    string
	dateInName         int
}

// getFilesSorted will look up all the files in the given folder,
// and return a list of files found sorted.
func getFilesSorted(logFolder string) ([]fileInfo, error) {

	// Get the names of all the log files.
	files := []fileInfo{}

	err := filepath.Walk(logFolder,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			fmt.Println(path, info.Size())

			fileName := filepath.Base(path)

			fi, err := os.Stat(path)
			if err != nil {
				return fmt.Errorf("error: failed to stat filbase: %v", path)
			}
			if fi.IsDir() {
				log.Printf(" * info: is directory, doing nothing: %v\n", fileName)
				return nil
			}

			fileDir := filepath.Dir(path)
			copiedFileRealPath := filepath.Join(fileDir, ".copied."+fileName)
			lockFileRealPath := filepath.Join(fileDir, ".lock."+fileName)

			// Check if it is a lock file, and if it is jump out since we don't work on them.
			_, err = os.Stat(lockFileRealPath)
			if err == nil {
				fmt.Printf(" * ITERATING FILES; FOUND LOCK FIlE, JUMPING OUT OF CURRENT WALK ITEM\n")
				return nil
			}

			fmt.Printf(" *** filebase contains: %+v\n", fileName)
			filebaseSplit := strings.Split(fileName, ".")
			fmt.Printf(" *** filebaseSplit contains: %+v\n", filebaseSplit)
			// If it does not contain an . we just skip the file.
			if len(filebaseSplit) < 2 {
				log.Printf("info: filename was to short, should be <yeardatetime>.<name>.., got: %#v\n", filebaseSplit)
				return nil
			}

			dateInt, err := strconv.Atoi(filebaseSplit[0])
			if err != nil {
				log.Printf("error: strconv.Atoi: %v\n", err)
				return nil
			}

			n := strings.Join(filebaseSplit[1:], ".")
			f := fileInfo{
				fileRealPath:       path,
				fileName:           fileName,
				fileDir:            fileDir,
				copiedFileRealPath: copiedFileRealPath,
				lockFileRealPath:   lockFileRealPath,
				nameWithoutDate:    n,
				dateInName:         dateInt,
			}

			files = append(files, f)

			return nil
		})
	if err != nil {
		return []fileInfo{}, err
	}

	// fmt.Printf("before sort: %+v\n", files)

	// Sort the files
	sort.SliceStable(files, func(i, j int) bool {
		return files[i].dateInName < files[j].dateInName
	})

	return files, nil
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
