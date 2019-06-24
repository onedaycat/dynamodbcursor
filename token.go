package dynamodbtoken

import (
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "encoding/base64"
)

//go:generate msgp
type CursorFields map[string]*string

func CreateToken(keys map[string]*dynamodb.AttributeValue) (string, error) {
    crs := make(CursorFields, len(keys))
    for key, value := range keys {
        crs[key] = value.S
    }

    cfByte, err := crs.MarshalMsg(nil)
    if err != nil {
        return "", err
    }

    return base64.URLEncoding.EncodeToString(cfByte), nil
}

func DecodeToken(token string) (map[string]*dynamodb.AttributeValue, error) {
    crs := decodeToken(token)

    dyAttr := map[string]*dynamodb.AttributeValue{}
    for k, v := range crs {
        dyAttr[k] = &dynamodb.AttributeValue{
            S: v,
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
