package dynamodbtoken

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/require"
)

func TestCreateNextToken(t *testing.T) {
	key1 := "rk_1"
	key2 := "hk_1"
	key3 := "note"
	expResult := map[string]*dynamodb.AttributeValue{
		"rk":      {S: &key1},
		"hk":      {S: &key2},
		"rkNoteV": {S: &key3},
	}

	token, err := CreateToken(expResult)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	result, err := DecodeToken(token)
	require.NoError(t, err)
	require.Equal(t, expResult, result)
}
