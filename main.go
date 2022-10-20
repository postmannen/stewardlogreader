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
	maxFileAge := flag.Int("maxFileAge", 60, "how old a single file is allowed to be in minutes before it gets read and sent to the steward socket")
	checkInterval := flag.Int("checkInterval", 5, "the check interval in minutes")
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
	go func() {
		for range ticker.C {

			files, err := getFilesSorted(*logFolder)
			if err != nil {
				log.Printf("%v\n", err)
				os.Exit(1)
			}

			for i := range files {
				age, err := fileIsHowOld(files[i].fullPath)
				if err != nil {
					log.Printf("%v\n", err)
					os.Exit(1)
				}

				if age > *maxFileAge {
					// TODO: Read the content, and create message with data, and send it here.
					fmt.Printf(" * file is older than maxAge, sending off file: %v\n", files[i])
					b, err := readFileContent(files[i].fullPath)
					if err != nil {
						log.Printf("%v\n", err)
						os.Exit(1)
					}
					fmt.Printf(" * read from file %v: %v\n", files[i].fullPath, string(b))

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
						fmt.Printf(" * read from file %v: %v\n", files[i].fullPath, string(b))

					}
				}
			}

		}
	}()

	<-sigCh

}

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

	modTime := (time.Now().Unix() - fi.ModTime().Unix()) / 60
	return int(modTime), nil
}

type fileAndDate struct {
	fullPath        string
	filebase        string
	nameWithoutDate string
	date            int
}

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
			filebaseSplit := strings.Split(filebase, ".")
			// If it does not contain an underscore we just skip the file.
			if len(filebaseSplit) < 2 {
				log.Printf("info: filename was to short, should be <yeardatetime>.<name>.., got: %v\n", filebaseSplit)
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

	fmt.Printf("before sort: %+v\n", files)

	// Sort the files
	sort.SliceStable(files, func(i, j int) bool {
		return files[i].date < files[j].date
	})

	return files, nil
}

func messageFileToSocket(socketFullPath string, messageFullPath string) error {
	socket, err := net.Dial("unix", socketFullPath)
	if err != nil {
		return fmt.Errorf(" * failed: could not open socket file for writing: %v", err)
	}
	defer socket.Close()

	fp, err := os.Open(messageFullPath)
	if err != nil {
		return fmt.Errorf(" * failed: could not open message file for reading: %v", err)
	}
	defer fp.Close()

	_, err = io.Copy(socket, fp)
	if err != nil {
		return fmt.Errorf("error: io.Copy failed: %v", err)
	}

	log.Printf("info: succesfully wrote message to socket\n")

	err = os.Remove(messageFullPath)
	if err != nil {
		return fmt.Errorf("error: os.Remove failed: %v", err)
	}

	return nil
}
