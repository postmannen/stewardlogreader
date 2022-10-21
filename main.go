package main

import (
	"flag"
	"fmt"
	"io"
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

	"gopkg.in/yaml.v3"
)

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

	ticker := time.NewTicker(time.Second * (time.Duration(*checkInterval)))
	defer ticker.Stop()

	go func() {
		for ; true; <-ticker.C {

			files, err := getFilesSorted(*logFolder)
			if err != nil {
				log.Printf("%v\n", err)
				os.Exit(1)
			}

			for i := range files {
				fmt.Println()

				age, err := fileIsHowOld(files[i].fullPath)
				if err != nil {
					log.Printf("%v\n", err)
					os.Exit(1)
				}

				if age > *maxFileAge {
					// TODO: Read the content, and create message with data, and send it here.
					fmt.Printf(" * file is older than maxAge: %v\n", files[i])

					err := readSendDeleteFile(msg, files[i], *socketFullPath)
					if err != nil {
						log.Printf("%v\n", err)
						os.Exit(1)
					}

					continue
				}

				fmt.Printf(" * age of file %v, is %v\n", files[i].fullPath, age)

				if i < len(files)-1 {
					// Since the files are sorted by the time written, we know that if there is
					// a file with the same name on the next spot it is an older file that we
					// want to send.
					if files[i].nameWithoutDate == files[i+1].nameWithoutDate {

						// TODO: Read the content, and create message with data, and send it here.
						fmt.Printf(" * sending off file: %v\n", files[i])
						b, err := readFileContent(files[i].fullPath)
						if err != nil {
							log.Printf("%v\n", err)
							os.Exit(1)
						}

						msg[0].Data = b

						err = readSendDeleteFile(msg, files[i], *socketFullPath)
						if err != nil {
							log.Printf("%v\n", err)
							os.Exit(1)
						}

					}
				}
			}

		}
	}()

	<-sigCh

}

// readSendDeleteFile is a wrapper function for the functions that
// will read File, send File and deletes the file.
func readSendDeleteFile(msg []Message, file fileAndDate, socketFullPath string) error {
	b, err := readFileContent(file.fullPath)
	if err != nil {
		return err
	}
	fmt.Printf(" * read from file %v: %v\n", file.fullPath, string(b))

	msg[0].Data = b
	msg[0].FileName = file.filebase
	err = messageToSocket(socketFullPath, msg, b)
	if err != nil {
		return err
	}

	fmt.Printf(" *** message %+v\n", msg)
	fmt.Printf(" **** msg.Data as string, file:%v, .data:%v\n", file.fullPath, string(b))

	// TODO: Delete the file here.
	err = os.Remove(file.fullPath)
	if err != nil {
		return fmt.Errorf("error: failed to remove the file: %v", err)
	}

	return nil
}

// readFileContent will read the content of the file, and return it as a []byte.
func readFileContent(fileName string) ([]byte, error) {
	fh, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("error: readFileContent failed to open file: %v", err)
	}
	defer fh.Close()

	b, err := io.ReadAll(fh)
	if err != nil {
		return nil, fmt.Errorf("error: readFileContent failed to read file: %v", err)
	}

	return b, nil

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

type fileAndDate struct {
	fullPath        string
	filebase        string
	nameWithoutDate string
	date            int
}

// getFilesSorted will look up all the files in the given folder,
// and return a list of files found sorted.
func getFilesSorted(logFolder string) ([]fileAndDate, error) {

	// Get the names of all the log files.
	files := []fileAndDate{}

	err := filepath.Walk(logFolder,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			fmt.Println(path, info.Size())

			filebase := filepath.Base(path)
			fi, err := os.Stat(path)
			if err != nil {
				return fmt.Errorf("error: failed to stat filbase: %v", path)
			}
			if fi.IsDir() {
				log.Printf(" * info: is directory, doing nothing: %v\n", filebase)
				return nil
			}

			fmt.Printf(" *** filebase contains: %+v\n", filebase)
			filebaseSplit := strings.Split(filebase, ".")
			fmt.Printf(" *** filebaseSplit contains: %+v\n", filebaseSplit)
			// If it does not contain an underscore we just skip the file.
			if len(filebaseSplit) < 2 {
				log.Printf("info: filename was to short, should be <yeardatetime>.<name>.., got: %#v\n", filebaseSplit)
				return nil
			}

			dateInt, err := strconv.Atoi(filebaseSplit[0])
			if err != nil {
				return err
			}

			n := strings.Join(filebaseSplit[1:], ".")
			f := fileAndDate{
				fullPath:        path,
				filebase:        filebase,
				nameWithoutDate: n,
				date:            dateInt,
			}

			files = append(files, f)

			return nil
		})
	if err != nil {
		return []fileAndDate{}, err
	}

	// fmt.Printf("before sort: %+v\n", files)

	// Sort the files
	sort.SliceStable(files, func(i, j int) bool {
		return files[i].date < files[j].date
	})

	return files, nil
}

// messageToSocket will write the message to the steward unix socket.
func messageToSocket(socketFullPath string, msg []Message, data []byte) error {
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
