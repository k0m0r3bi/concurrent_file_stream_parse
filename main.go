package main

import (
	"flag"
	"bufio"
	"sync"
	"log"
	"fmt"
	"os"
	"io"
	"strings"
	"time"
	"strconv"
)


// SETUP
// +++++++++++++++++++++++++++++++++++++++++++++

var fname *string = flag.String("file", "FILE-PATH", "Incoming file")

var outdir string = "OUTDIR"

const mb 		= 1024 * 1024
const gb 		= 1024 * mb

var readerSync 		sync.WaitGroup						// to keep track of reader bees go routines
const readerCount 	= 10
var readers 		= make([]*Reader, readerCount)

const maxBufR int64 	= 10 * mb 						// Reader worker buffer size

var writerSync 		sync.WaitGroup						// to keep track of worker bees go routines
const writerCount 	= 10
var writers 		= make([]*Writer, writerCount)

const maxBufW int64 	= 500 * mb 						// Writer worker buffer size (out file size)

var offsetChan 		= make(chan int64, readerCount)				// values in this chan indicates to zorkers where they have to start reading the file.
var artChan		= make(chan []string, readerCount)

var offsetTrack		= make(chan (int64))

var keys		= []string{"computer", "science", "algorithm", "hardware", "software", "data mining", "database", "operating system", "daemon", "file format", "cryptography", "computing", "concurrency", 
								"artificial intelligence", "compiler", "code", "binary", "cipher",
								"mathematic", "algebra", "number", "conjecture", " pi "} 



// WORKERS
// +++++++++++++++++++++++++++++++++++++++++++++

type Reader struct {
  fsize int64
}

func NewReader(fsize int64) (w *Reader) {
	return &Reader{
		fsize: fsize,							// readers will suicide when asked to read past file size
	}
}

type Writer struct {
  	root 	string
  	buffer  []string
  	pointer int
}

func NewWriter(id int) (w *Writer) {
	return &Writer{
		root: outdir + strconv.Itoa(id) + "_",
		buffer: make([]string, 0),
	}
}



// INIT
// +++++++++++++++++++++++++++++++++++++++++++++

func init() {

	flag.Parse()
	switch {
	case *fname == "":
		log.Fatal("Must provide a file!\n")
	}

	f, e := os.Stat(*fname)
	if e != nil {
	    panic(e)
	}
	fsize := f.Size()

	for i := 0; i < readerCount; i++ {					// initialize readers. Thwy will be listening for events on the offsetChan channel
		readerSync.Add(1)
		readers[i] = NewReader(fsize)					// they receive fsize as argument -> they know zhen to stop reading
		go readers[i].Read(offsetChan, maxBufR, *fname, artChan)
	}

	for i := 0; i < writerCount; i++ {					// initialize writer. Thwy will be listening for articles on the artChan channel
		writerSync.Add(1)
		writers[i] = NewWriter(i)
		go writers[i].Save(artChan)
	}


	go func() {
		var sum int64 = 0
		for {
			curOffset := <- offsetTrack
			sum += curOffset
			fmt.Printf("Parsed : %v mb\n", sum/mb)
		}
	}()
}



// CORE
// +++++++++++++++++++++++++++++++++++++++++++++
func main() {

	start := time.Now()


	var i int64         							// initialize events => Awake readers by feeding offsetChan
	for i = 0; i < readerCount; i++ {
		offsetChan <- i * maxBufR
	}

	readerSync.Wait()							// wait for all go routine readers to complete
	writerSync.Wait()

	close(offsetChan)
	close(artChan)

	elapsed := time.Since(start)
	log.Printf("Parsing took %s", elapsed)
}





// JOBS
// +++++++++++++++++++++++++++++++++++++++++++++

func (w *Reader) Read(offsetChan chan int64, maxBufR int64, fname string, artChan chan []string) {

	defer readerSync.Done()

	f, e := os.Open(fname)
	if e != nil {
		panic(e)
	}
	defer f.Close()

	for {

		offset := <- offsetChan

		if offset > w.fsize {						// means EOF, return to close routine
			return
		}
		
		f.Seek(offset, 0)						// Move the pointer of the file to the start of designated chunk.

		r := bufio.NewReader(f)

		if offset != 0 {						// Move to a few bytes (till end of word) if offset in the middle of a word 
			_, e = r.ReadBytes(' ')
			if e == io.EOF {
				fmt.Println("EOF")
				return
			}
			if e != nil {
				panic(e)
			}
		}

		var localBuf int64

		outBuff := make([]string, 0)

		for {

			if localBuf > maxBufR {					// Break if read size has exceed the chunk size.

				artChan <- outBuff				// send article to channel for post processing
				offsetChan <- offset + localBuf  + 1		// send another offset to read in the readers chan
				offsetTrack <- offset + localBuf  + 1
				break
			}

			b, e := r.ReadBytes(' ')

			if e == io.EOF {					// Break if end of file is encountered.
				break
			}

			if e != nil {
				panic(e)
			}

			localBuf += int64(len(b))

			w := strings.TrimSpace(string(b))

			if strings.Contains(w, "<page>") {			// caught article start

				outBuff = append(outBuff, w)			// append word into output buffer

				for {						// looping bytes until article end </page>

					b, e := r.ReadBytes(' ')
					if e == io.EOF {			// Break if end of file is encountered.
						break
					}
					if e != nil {
						panic(e)
					}
					localBuf += int64(len(b))

					w := strings.TrimSpace(string(b))
					outBuff = append(outBuff, w)		// append all subsequent words into output buffer

					if strings.Contains(w, "</page>") {	// caught article end
						break
					}
				}
			}
		}
	}
}


func (w *Writer) Save(artChan chan []string) {

	defer writerSync.Done()

	for {

		article := <- artChan
		text := strings.Join(article, " ")
		text = strings.ToLower(text)
		

		for i := range keys {
			c := strings.Count(text, keys[i])
			switch c {
			case 0:
				fmt.Printf("fond  0 match for %+v\n", keys[i])
				continue
			default:
				fmt.Printf("fond %vmatch for %+v\n",c, keys[i])
				w.buffer = append(w.buffer, "\n" + text)
				rune := []rune(text)
				w.pointer += len(rune)
				break
			}
		}
		
		switch {
		case w.pointer > 10000:

			f, err := os.OpenFile(w.root + strconv.FormatInt(time.Now().UnixNano(), 10) + ".txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600);
			if err != nil {
				panic(err)
			}
			if _, err = f.WriteString(strings.Join(w.buffer, "")); err != nil {
				panic(err)
			}
			w.buffer = make([]string, 0)
			w.pointer = 0
			defer f.Close()
		default:
			continue
		}
	}
}
