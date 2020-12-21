package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/giridharmb/grpc-messagepb"
	"google.golang.org/grpc"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func errCheckFatal(e error, msg string) {
	if e != nil {
		log.Fatalf("\n(ERROR) : %v : %v", msg, e.Error())
	}
}

func errCheckPrint(e error, msg string) {
	if e != nil {
		log.Printf("\n(ERROR) : %v : %v", msg, e.Error())
	}
}

////////// Progress Bar - start /////////////

var bar Bar

type Bar struct {
	percent int64  // progress percentage
	cur     int64  // current progress
	total   int64  // total value for progress
	rate    string // the actual progress bar to be printed
	graph   string // the fill value for progress bar
}

func (bar *Bar) getPercent() int64 {
	return int64(float32(bar.cur) / float32(bar.total) * 100)
}

func (bar *Bar) NewOptionWithGraph(start, total int64, graph string) {
	bar.graph = graph
	bar.NewOption(start, total)
}

func (bar *Bar) Play(cur int64) {
	bar.cur = cur
	last := bar.percent
	bar.percent = bar.getPercent()
	if bar.percent != last && bar.percent%2 == 0 {
		bar.rate += bar.graph
	}
	fmt.Printf("\r[%-50s]%3d%% %8d/%d", bar.rate, bar.percent, bar.cur, bar.total)
}

func (bar *Bar) NewOption(start, total int64) {
	bar.cur = start
	bar.total = total
	if bar.graph == "" {
		bar.graph = "#"
	}
	bar.percent = bar.getPercent()
	for i := 0; i < int(bar.percent); i += 2 {
		bar.rate += bar.graph // initial progress position
	}
}

func (bar *Bar) Finish() {
	fmt.Println()
}

func checkHostPort(host string, port string) bool {
	timeout := 3 * time.Second
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), timeout)
	if err != nil {
		fmt.Printf("\nCould not establish connection to host (%v) and port (%v) : %v", host, port, err.Error())
		return false
	}
	if conn != nil {
		defer func() {
			_ = conn.Close()
		}()
		//fmt.Println("Opened", net.JoinHostPort(host, port))
	}
	return true
}

////////// Progress Bar - end /////////////

func main() {

	bar.NewOption(0, 100)

	var server string
	flag.StringVar(&server, "server", "remote-host-1.company.com", "server hostname")

	var fileName string
	flag.StringVar(&fileName, "file", "file.txt", "name of the file to transfer")

	var port string
	flag.StringVar(&port, "port", "8080", "remote port on the server")

	flag.Parse()

	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		fmt.Printf("(ERROR) : (%v) does not exist", fileName)
		return
	}

	hostAndPort := fmt.Sprintf("%v:%v", server, port)

	conn, err := grpc.Dial(hostAndPort, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect to server : %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	connectionValid := checkHostPort(server, port)
	if connectionValid == false {
		return
	}

	client := messagepb.NewMyDataServiceClient(conn)

	//doUnary(client)
	//
	//doServerStreaming(client)
	//
	//doClientStreaming(client)
	//
	//doBidirectionalStreaming(client)

	doBidirectionalDataTransfer(client, server, fileName)

}

func StringWithCharset(length int, charset string) string {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, charset)
}

func doUnary(client messagepb.MyDataServiceClient) {
	fmt.Printf("\nstarting to do a unary rpc...")

	request := &messagepb.SumRequest{
		NumberFirst:  3931,
		NumberSecond: 99,
	}

	response, err := client.GetSum(context.Background(), request)
	if err != nil {
		fmt.Printf("\nerror while calling the GetSum function : %v", err)
	}
	fmt.Printf("\nSum of the numbers in doUnary func : %v", response.SumResult)
}

func doServerStreaming(client messagepb.MyDataServiceClient) {

	fmt.Printf("\nstarting to do server streaming rpc...")

	myData := &messagepb.Data{
		FirstName: "Giridhar",
		LastName:  "Bhujanga",
		Age:       40,
	}

	request := &messagepb.Request{Data: myData}

	resultSteaming, err := client.FetchData(context.Background(), request)
	if err != nil {
		log.Fatalf("\nerror while calling server streaming rpc : %v", err)
	}
	fmt.Printf("\nnow trying to fetch data via rpc stream...")
	for {
		msg, err := resultSteaming.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("\nerror while reading from data stream : %v", err)
		}
		fmt.Printf("\nresponse read from data streaming server : %v", msg.GetResult())
	}
}

func doClientStreaming(client messagepb.MyDataServiceClient) {
	fmt.Printf("\nstarting to do client streaming rpc...")

	stream, err := client.ClientStream(context.Background())
	if err != nil {
		log.Fatalf("\nerror while calling doCleintStreaming : %v", err)
	}

	requests := make([]*messagepb.DataRequestClientStream, 0)
	for i := 0; i < 15; i++ {
		myRandomString := StringWithCharset(10, charset)
		myIndex := int32(i)

		myData := &messagepb.DataRequestClientStream{
			RandomString: myRandomString,
			Index:        myIndex,
		}
		requests = append(requests, myData)

	}

	for _, req := range requests {
		fmt.Printf("\nsending data...")
		_ = stream.Send(req)
		time.Sleep(300 * time.Millisecond)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("\nerror while receiving response from server :%v", err)
	}
	fmt.Printf("\nfinal response from server : %v", resp)

}

func doBidirectionalStreaming(client messagepb.MyDataServiceClient) {
	fmt.Printf("\nBIDI : starting to do bi-directional streaming rpc...")

	stream, err := client.BDStream(context.Background())
	if err != nil {
		log.Fatalf("\nBIDI : error while creating stream : %v", err)
		return
	}

	waitChannel := make(chan struct{})

	// populate data first to send to server
	requests := make([]*messagepb.BDStreamMessageRequest, 0)
	for i := 0; i < 15; i++ {
		firstName := StringWithCharset(10, charset)
		lastName := StringWithCharset(10, charset)

		myData := &messagepb.BDStreamMessageRequest{
			FirstName: firstName,
			LastName:  lastName,
		}
		requests = append(requests, myData)
	}

	// function to send a bunch of messages
	go func() {
		for _, req := range requests {
			fmt.Printf("\nBIDI : Sending message : %v", req)
			_ = stream.Send(req)
			time.Sleep(1000 * time.Millisecond)
		}
		_ = stream.CloseSend()
	}()

	// function to receivce a bunh of messages
	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf("\nBIDI : error while receiving : %v", err)
				break
			}
			fmt.Printf("\nBIDI : received : %v", res.GetHash())
		}
		close(waitChannel)
	}()

	// block until everything is done
	<-waitChannel
}

type readBytesData struct {
	readBytes      []byte
	totalBytesRead int64
	totalFileSize  int64
}

func getFileSize(filePath string) (int64, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return -1, err
	}
	size := fi.Size()
	return size, nil
}

func readFileInChunks(fileName string, bytesChannel chan []byte) error {
	const BufferSize = 4096
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("(ERROR) : %v", err.Error())
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	completeFileSize := 0
	for {
		buffer := make([]byte, BufferSize)
		totalBytesRead, err := file.Read(buffer)
		if err == io.EOF {
			// fmt.Printf("\nFile Scan Complete.\n")
			close(bytesChannel)
			break
		}
		if err != nil {
			fmt.Printf("(ERROR) : %v", err.Error())
			return err
		}
		//fmt.Printf("\ntotalBytesRead: %v", totalBytesRead)
		completeFileSize = completeFileSize + totalBytesRead
		//fmt.Printf("\ncompleteFileSize : %v", completeFileSize)
		bytesChannel <- buffer[:totalBytesRead]

	}
	return nil
}

func doBidirectionalDataTransfer(client messagepb.MyDataServiceClient, server string, fileName string) {
	//fmt.Printf("\nBIDI : starting to do bi-directional streaming rpc...")

	stream, err := client.BDTransfer(context.Background())
	if err != nil {
		log.Fatalf("\nBIDI_TRANSFER : error while creating stream : %v", err)
		return
	}

	//fileName := "random_data.bin"

	readBytesChan := make(chan []byte)

	waitChannel := make(chan struct{})

	totalSizeBytes, _ := getFileSize(fileName)

	fmt.Println("Uploading...")

	barGraphValueChannel := make(chan int64)

	//////////////////////////////////////////////////////

	go func() {
		for {
			bytesArray, ok := <-readBytesChan
			if !ok {
				break
			}

			myRequest := &messagepb.BDTransferMessage{
				Data:           bytesArray,
				FileName:       fileName,
				BytesTotalSize: totalSizeBytes,
			}
			//time.Sleep(10 * time.Millisecond)
			_ = stream.Send(myRequest)
		}
		_ = stream.CloseSend()
	}()

	go func() {
		_ = readFileInChunks(fileName, readBytesChan)
	}()

	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf("\nBIDI_TRANSFER : error while receiving : %v", err)
				break
			}

			//ceilValue := int(res.GetPercentComplete())
			//fmt.Printf("\rTransferred : %3d%%", ceilValue)

			value := int64(math.Ceil(float64(res.GetPercentComplete())))

			barGraphValueChannel <- value

		}
		close(barGraphValueChannel)
		close(waitChannel)
	}()

	go func() {
		for {
			value, ok := <-barGraphValueChannel
			if !ok {
				break
			}
			bar.Play(value)
		}
	}()

	<-barGraphValueChannel
	<-waitChannel

	fmt.Printf("\nDone With File Transfer.")

}
