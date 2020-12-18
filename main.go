package main

import (
	"context"
	"fmt"
	"github.com/giridharmb/grpc-messagepb"
	"google.golang.org/grpc"
	"io"
	"log"
	"math/rand"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func main() {
	fmt.Println("Iam a client")

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect to server : %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	client := messagepb.NewMyDataServiceClient(conn)

	fmt.Printf("Created client : %f", client)

	doUnary(client)

	doServerStreaming(client)

	doClientStreaming(client)

	doBidirectionalStreaming(client)

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
