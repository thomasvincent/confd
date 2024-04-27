package dynamodb

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Client is a wrapper around the DynamoDB client
type Client struct {
	client *dynamodb.DynamoDB
	table  string
	logger log.Logger
}

// NewDynamoDBClient creates a new Client instance
func NewDynamoDBClient(table string, logger log.Logger) (*Client, error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-west-2")}, nil)
	if err != nil {
		return nil, err
	}
	dynamoDBSvc := dynamodb.New(sess)

	// Check if the table exists
	_, err = dynamoDBSvc.DescribeTable(&dynamodb.DescribeTableInput{TableName: &table})
	if err != nil {
		return nil, err
	}

	return &Client{dynamoDBSvc, table, logger}, nil
}

// GetValues retrieves values from DynamoDB
func (c *Client) GetValues(ctx context.Context, keys []string) (map[string]string, error) {
	vars := make(map[string]string)
	for _, key := range keys {
		g, err := c.client.GetItemWithContext(ctx, &dynamodb.GetItemInput{Key: map[string]*dynamodb.AttributeValue{"key": {S: aws.String(key)}}, TableName: &c.table})
		if err != nil {
			return vars, err
		}
		vars[key] = *g.Item["value"].S
	}
	return vars, nil
}

// WatchPrefix watches a DynamoDB prefix for changes
func (c *Client) WatchPrefix(ctx context.Context, prefix string, keys []string, waitIndex uint64, stopChan chan bool) (uint64, error) {
	<-stopChan
	return 0, nil
}

func main() {
	logger := log.DefaultLogger
	tableName := "my-table"
	client, err := NewDynamoDBClient(tableName, logger)
	if err != nil {
		log.Fatal(err)
	}

	keys := []string{"key1", "key2"}
	values, err := client.GetValues(context.TODO(), keys)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(values)
}
