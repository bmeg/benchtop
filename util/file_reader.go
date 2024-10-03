package util

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"os"
	"strings"
)

func LineCounter(file string) (int, error) {
	var fh io.ReadCloser
	var err error
	fh, err = os.Open(file)
	if err != nil {
		return 0, err
	}

	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := fh.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

// StreamLines returns a channel of lines from a file.
func StreamLines(file string, chanSize int) (chan string, error) {

	var fh io.ReadCloser
	var err error
	fh, err = os.Open(file)
	if err != nil {
		return nil, err
	}
	var scanner *bufio.Scanner

	if strings.HasSuffix(file, ".gz") {
		gz, err := gzip.NewReader(fh)
		if err != nil {
			return nil, err
		}
		scanner = bufio.NewScanner(gz)
	} else {
		scanner = bufio.NewScanner(fh)
	}

	const maxCapacity = 16 * 1024 * 1024
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxCapacity)

	lineChan := make(chan string, chanSize)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			lineChan <- line
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Error: %s", err)
		}
		close(lineChan)
		fh.Close()
	}()

	return lineChan, nil
}
