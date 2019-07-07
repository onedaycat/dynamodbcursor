package dynamodbcursor

import (
	"encoding/base64"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

//go:generate msgp
type CursorFields map[string]*AttributeValue

type AttributeValue struct {
	BOOL *bool   `msg:"b"`
	N    *string `msg:"n"`
	NULL *bool   `msg:"nl"`
	S    *string `msg:"s"`
}

func CreateToken(limit int, resItems []map[string]*dynamodb.AttributeValue, lastEvaluatedKey map[string]*dynamodb.AttributeValue) ([]map[string]*dynamodb.AttributeValue, string, error) {
	if len(resItems) > limit {
		resItems = resItems[:limit]
	} else {
		return resItems, "", nil
	}

	if lastEvaluatedKey == nil || len(lastEvaluatedKey) == 0 {
		return resItems, "", nil
	}

	last := resItems[len(resItems)-1]
	crs := make(CursorFields, len(lastEvaluatedKey))
	for key, value := range lastEvaluatedKey {
		switch {
		case value.BOOL != nil:
			crs[key] = &AttributeValue{BOOL: last[key].BOOL}
		case value.N != nil:
			crs[key] = &AttributeValue{N: last[key].N}
		case value.NULL != nil:
			crs[key] = &AttributeValue{NULL: last[key].NULL}
		case value.S != nil:
			crs[key] = &AttributeValue{S: last[key].S}
		default:
			return nil, "", fmt.Errorf("%s is use unspported type for cursor", key)
		}
	}

	cfByte, err := crs.MarshalMsg(nil)
	if err != nil {
		return nil, "", err
	}

	return resItems, base64.URLEncoding.EncodeToString(cfByte), nil
}

func DecodeToken(token string) (map[string]*dynamodb.AttributeValue, error) {
	if token == "" {
		return nil, nil
	}

	crs := decodeToken(token)

	dyAttr := map[string]*dynamodb.AttributeValue{}
	for k, v := range crs {
		switch {
		case v.BOOL != nil:
			dyAttr[k] = &dynamodb.AttributeValue{BOOL: v.BOOL}
		case v.N != nil:
			dyAttr[k] = &dynamodb.AttributeValue{N: v.N}
		case v.NULL != nil:
			dyAttr[k] = &dynamodb.AttributeValue{NULL: v.NULL}
		case v.S != nil:
			dyAttr[k] = &dynamodb.AttributeValue{S: v.S}
		default:
			return nil, fmt.Errorf("%s is use unspported type for cursor", k)
		}
	}

	return dyAttr, nil
}

func decodeToken(token string) CursorFields {
	cfByte, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return nil
	}

	cf := CursorFields{}
	if _, err = cf.UnmarshalMsg(cfByte); err != nil {
		return nil
	}

	return cf
}
