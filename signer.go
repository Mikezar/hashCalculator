package main

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func startJob(jobFunc job, in, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(out)

	start := time.Now()

	jobFunc(in, out)

	log.Printf("%v", time.Since(start).Milliseconds())
}

// ExecutePipeline executes the series of jobs
func ExecutePipeline(jobs ...job) {
	var wg = &sync.WaitGroup{}

	var chIn = make(chan interface{})

	for j := 0; j < len(jobs); j++ {
		wg.Add(1)

		var chOut = make(chan interface{})
		go startJob(jobs[j], chIn, chOut, wg)
		chIn = chOut
	}

	wg.Wait()
}

// SingleHash computes hash applying the formula crc32(data)+"~"+crc32(md5(data))
func SingleHash(in, out chan interface{}) {
	var wg = &sync.WaitGroup{}

	for data := range in {
		formatedData := fmt.Sprintf("%v", data)
		md5Hash := DataSignerMd5(formatedData)

		wg.Add(1)

		go func(data string, md5 string, wg *sync.WaitGroup) {
			defer wg.Done()

			crcChan := make(chan string)
			md5Chan := make(chan string)

			go calculateCrcHash(data, crcChan)
			go calculateCrcHash(md5, md5Chan)

			rightPart := <-md5Chan
			leftPart := <-crcChan
			out <- leftPart + "~" + rightPart
		}(formatedData, md5Hash, wg)
	}

	wg.Wait()
}

func calculateCrcHash(data string, out chan string) {
	out <- DataSignerCrc32(data)
}

//MultiHash computes the hsh ovtained by SingleHash and extends it up to 6 times where subhash is crc(inx + data), then concatenates
func MultiHash(in, out chan interface{}) {
	var wg = &sync.WaitGroup{}

	for data := range in {
		wg.Add(1)
		formatedData := fmt.Sprintf("%v", data)

		go calculateMultiHash(formatedData, wg, out)
	}

	wg.Wait()
}

func calculateMultiHash(data string, wg *sync.WaitGroup, out chan interface{}) {
	var tempChannel = make(chan Hash)
	var innerWg = &sync.WaitGroup{}

	for i := 0; i < 6; i++ {
		innerWg.Add(1)

		go func(data string, iwg *sync.WaitGroup, indx int) {
			defer iwg.Done()

			var ch = make(chan string)

			go calculateCrcHash(strconv.Itoa(indx)+data, ch)

			hashElement := Hash{
				Number:  indx,
				Payload: <-ch,
			}

			close(ch)

			tempChannel <- hashElement
		}(data, innerWg, i)
	}

	go func(ch chan Hash, iwg *sync.WaitGroup) {
		defer close(ch)

		iwg.Wait()
	}(tempChannel, innerWg)

	go processMultiHash(tempChannel, wg, out)

	wg.Wait()
}

func processMultiHash(hashChannel chan Hash, wg *sync.WaitGroup, out chan interface{}) {
	defer wg.Done()

	hashArray := HashArray{}

	for h := range hashChannel {
		hashArray = append(hashArray, h)
	}

	sort.Sort(hashArray)
	out <- hashArray.Concat()
}

// CombineResults gets all previously computed hashes, sort it out and then concatenates with _
func CombineResults(in, out chan interface{}) {
	var hashArray []string

	for h := range in {
		hashArray = append(hashArray, h.(string))
	}

	sort.Strings(hashArray)
	out <- strings.Join(hashArray, "_")
}
