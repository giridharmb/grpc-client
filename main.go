package main

import (
	"context"
	"fmt"
	"github.com/giridharmb/grpc-messagepb"
	"google.golang.org/grpc"
	"io"
	"log"
)

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
