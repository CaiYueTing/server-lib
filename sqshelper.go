package serverlib

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	uuid "github.com/satori/go.uuid"
)

type Queue struct {
	region *string
	url    *string
}

type Qmessage struct {
	Msg           *string
	ReceiptHandle *string
}

func New(region string, url string) *Queue {
	return &Queue{
		region: &region,
		url:    &url,
	}
}

func newSvc(region *string) *sqs.SQS {
	return sqs.New(session.Must(session.NewSession(&aws.Config{
		Region: region,
	})))
}

func (q *Queue) newSendMessage(msg string) *sqs.SendMessageInput {
	u, _ := uuid.NewV4()
	return &sqs.SendMessageInput{
		DelaySeconds:           aws.Int64(0),
		MessageGroupId:         aws.String("GroupId"),
		MessageDeduplicationId: aws.String(u.String()),
		MessageBody:            &msg,
		QueueUrl:               q.url,
	}
}

func (q *Queue) newReceiveMessage(msgnumber int64, inflight int64) *sqs.ReceiveMessageInput {
	return &sqs.ReceiveMessageInput{
		QueueUrl:            q.url,
		MaxNumberOfMessages: &msgnumber,
		VisibilityTimeout:   &inflight,
		WaitTimeSeconds:     aws.Int64(0),
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
	}
}

func (q *Queue) newDeleteMessage(receiptHandle *string) *sqs.DeleteMessageInput {
	return &sqs.DeleteMessageInput{
		QueueUrl:      q.url,
		ReceiptHandle: receiptHandle,
	}
}

func (q *Queue) SendMessage(msg string) error {
	input := q.newSendMessage(msg)
	svc := newSvc(q.region)
	result, err := svc.SendMessage(input)

	if err != nil {
		log.Panic(err)
	}
	defer recoverfunc(err, "send message to queue")

	fmt.Println("send message success", result.MessageId)
	return nil
}

func (q *Queue) ReceiveMessage(msgnumber int64, inflight int64) *[]Qmessage {
	input := q.newReceiveMessage(msgnumber, inflight)
	svc := newSvc(q.region)
	result, err := svc.ReceiveMessage(input)

	if err != nil {
		log.Panic(err)
	}
	defer recoverfunc(err, "receive message")

	fmt.Println("Message receive amount: ", len(result.Messages))
	return msg2Struct(result.Messages)
}

func msg2Struct(msgs []*sqs.Message) *[]Qmessage {
	messages := make([]Qmessage, len(msgs))
	for i, msg := range msgs {
		messages[i].Msg = msg.Body
		messages[i].ReceiptHandle = msg.ReceiptHandle
	}
	return &messages
}

func (q *Queue) Delete(receiptHandle *string) {
	input := q.newDeleteMessage(receiptHandle)
	svc := newSvc(q.region)
	_, err := svc.DeleteMessage(input)

	if err != nil {
		log.Panic(err)
	}
	defer recoverfunc(err, "delete message")

	fmt.Println("Message delete")
}

func recoverfunc(err error, method string) {
	if err := recover(); err != nil {
		fmt.Println("recover"+method, err)
	}
}
