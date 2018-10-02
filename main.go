package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"math/big"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

const batch = 1000000
const workers = 9

func main() {
	primes := make(chan int)
	var wg sync.WaitGroup
	wg.Add(workers)

	startClock := time.Now()

	start := 0
	if _, err := os.Stat(fileName); !os.IsNotExist(err) {
		last, err := ReadLast()
		if err != nil {
			log.Fatalf("error reading last written prime err: %v", err)
			return
		}
		start = last + 1
	}
	batches := start
	for i := 0; i < workers; i++ {
		go worker(batches, &wg, primes)
		batches += batch
	}

	saveDone := make(chan bool)
	go func() {
		var list []int
		for prime := range primes {
			list = append(list, prime)
		}

		fmt.Printf("%v-%v found %v primes in %v seconds ", start, (workers * batches), len(list), time.Since(startClock))
		sort.Ints(list)
		err := save(list)
		if err != nil {
			log.Fatalf("unable to store primes err: %v", err)
		}
		saveDone <- true
	}()

	wg.Wait()
	close(primes)
	<-saveDone
}

func worker(start int, wg *sync.WaitGroup, prime chan<- int) {
	defer wg.Done()
	batchSize := start + batch

	if start < 3 {
		//2 is only even prime number
		prime <- 2
		start = 3
	}

	if start%2 == 0 {
		//if we start on even, step ahead since all prime numbers except two is odd
		start++
	}
	for i := start; i < batchSize; i += 2 {
		if big.NewInt(int64(i)).ProbablyPrime(20) {
			prime <- i
		}
	}
}

const fileName = "primes.csv"

func ReadLast() (int, error) {
	f, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return -1, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return -1, err
	}

	buf := make([]byte, 512)
	n, err := f.ReadAt(buf, fi.Size()-int64(len(buf)))
	if err != nil {
		return -1, err
	}
	buf = buf[:n]
	sc := bufio.NewScanner(bytes.NewReader(buf))
	var lastLine string
	for sc.Scan() {
		txt := sc.Text()
		if txt != "" {
			lastLine = txt
		}
	}
	if err := sc.Err(); err != nil {
		return -1, err
	}
	return strconv.Atoi(lastLine)
}

func save(prime []int) error {
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}

	defer f.Close()
	w := csv.NewWriter(f)

	for _, nb := range prime {
		record := []string{strconv.Itoa(nb)}
		if err := w.Write(record); err != nil {
			return fmt.Errorf("error writing record to csv: %v", err)
		}
	}
	w.Flush()
	return w.Error()
}
