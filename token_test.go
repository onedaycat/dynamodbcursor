package dynamodbcursor

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/stretchr/testify/require"
)

type testData struct {
	Key string `json:"key"`
}

func createDynamodbAttribute(data []*testData) []map[string]*dynamodb.AttributeValue {
	result := make([]map[string]*dynamodb.AttributeValue, len(data))
	for i, d := range data {
		result[i], _ = dynamodbattribute.MarshalMap(d)
	}

	return result
}

func createLastKey(key string) map[string]*dynamodb.AttributeValue {
	if key == "" {
		return nil
	}

	return map[string]*dynamodb.AttributeValue{
		"key": {S: &key},
	}
}

func TestCreateToken(t *testing.T) {
	data := []*testData{
		{Key: "1"},
		{Key: "2"},
		{Key: "3"},
		{Key: "4"},
	}

	t.Run("data more than limit", func(t *testing.T) {
		resItem := createDynamodbAttribute(data)
		lastKey := createLastKey("4")

		resItem, next, err := CreateToken(3, resItem, lastKey)
		require.NoError(t, err)
		require.NotEmpty(t, next)
		evalKey, err := DecodeToken(next)
		require.NoError(t, err)
		require.Equal(t, createLastKey("3"), evalKey)
		require.Len(t, resItem, 3)
	})

	t.Run("data less than limit", func(t *testing.T) {
		resItem := createDynamodbAttribute(data)
		lastKey := createLastKey("4")

		resItem, next, err := CreateToken(10, resItem, lastKey)
		require.NoError(t, err)
		require.Empty(t, next)
		require.Len(t, resItem, 4)
	})

	t.Run("data equal limit", func(t *testing.T) {
		resItem := createDynamodbAttribute(data)
		lastKey := createLastKey("4")

		resItem, next, err := CreateToken(4, resItem, lastKey)
		require.NoError(t, err)
		require.Empty(t, next)
		require.Len(t, resItem, 4)
	})

	t.Run("data equal limit 1", func(t *testing.T) {
		data = []*testData{
			{Key: "1"},
		}

		resItem := createDynamodbAttribute(data)
		lastKey := createLastKey("1")

		resItem, next, err := CreateToken(1, resItem, lastKey)
		require.NoError(t, err)
		require.Empty(t, next)
		require.Len(t, resItem, 1)
	})

	t.Run("data more than limit 1", func(t *testing.T) {
		data = []*testData{
			{Key: "1"},
			{Key: "2"},
		}

		resItem := createDynamodbAttribute(data)
		lastKey := createLastKey("2")

		resItem, next, err := CreateToken(1, resItem, lastKey)
		require.NoError(t, err)
		require.NotEmpty(t, next)
		evalKey, err := DecodeToken(next)
		require.NoError(t, err)
		require.Equal(t, createLastKey("1"), evalKey)
		require.Len(t, resItem, 1)
	})
}
