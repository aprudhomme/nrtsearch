package luceneserver;

import (
    "fmt"
    "testing"

    "github.com/golang/protobuf/jsonpb"
)

var (
    emptyJson = "{}"
    emptyMessage = JsonStruct{}

    simpleJson = "{\"b\":true,\"d\":2.345,\"f\":1.123,\"i\":100,\"l\":1001,\"n\":null,\"s\":\"test_str\"}"
    simpleMessage = JsonStruct{
        Fields: map[string]*JsonValue{
            "s": &JsonValue{
                Kind: &JsonValue_StringValue{
                    StringValue: "test_str",
                },
            },
            "b": &JsonValue{
                Kind: &JsonValue_BoolValue{
                    BoolValue: true,
                },
            },
            "i": &JsonValue{
                Kind: &JsonValue_LongValue{
                    LongValue: 100,
                },
            },
            "l": &JsonValue{
                Kind: &JsonValue_LongValue{
                    LongValue: 1001,
                },
            },
            "f": &JsonValue{
                Kind: &JsonValue_DoubleValue{
                    DoubleValue: 1.123,
                },
            },
            "d": &JsonValue{
                Kind: &JsonValue_DoubleValue{
                    DoubleValue: 2.345,
                },
            },
            "n": &JsonValue{
                Kind: &JsonValue_NullValue{
                    NullValue: JsonNullValue_NULL_VALUE,
                },
            },
        },
    }

    listJson = "{\"b\":true,\"i\":100,\"list\":[1001,1.123,2.345,null,\"test_str\"]}"
    listMessage = JsonStruct{
        Fields: map[string]*JsonValue{
            "b": &JsonValue{
                Kind: &JsonValue_BoolValue{
                    BoolValue: true,
                },
            },
            "i": &JsonValue{
                Kind: &JsonValue_LongValue{
                    LongValue: 100,
                },
            },
            "list": &JsonValue{
                Kind: &JsonValue_ListValue{
                    ListValue: &JsonListValue{
                        Values: []*JsonValue{
                            &JsonValue{
                                Kind: &JsonValue_LongValue{
                                    LongValue: 1001,
                                },
                            },
                            &JsonValue{
                                Kind: &JsonValue_DoubleValue{
                                    DoubleValue: 1.123,
                                },
                            },
                            &JsonValue{
                                Kind: &JsonValue_DoubleValue{
                                    DoubleValue: 2.345,
                                },
                            },
                            &JsonValue{
                                Kind: &JsonValue_NullValue{
                                    NullValue: JsonNullValue_NULL_VALUE,
                                },
                            },
                            &JsonValue{
                                Kind: &JsonValue_StringValue{
                                    StringValue: "test_str",
                                },
                            },
                        },
                    },
                },
            },
        },
    }

    complexListJson = "{\"b\":true,\"i\":100,\"list\":[1001,[1.123,2.345],{\"n\":null,\"s\":\"test_str\"}]}"
    complexListMessage = JsonStruct{
        Fields: map[string]*JsonValue{
            "b": &JsonValue{
                Kind: &JsonValue_BoolValue{
                    BoolValue: true,
                },
            },
            "i": &JsonValue{
                Kind: &JsonValue_LongValue{
                    LongValue: 100,
                },
            },
            "list": &JsonValue{
                Kind: &JsonValue_ListValue{
                    ListValue: &JsonListValue{
                        Values: []*JsonValue{
                            &JsonValue{
                                Kind: &JsonValue_LongValue{
                                    LongValue: 1001,
                                },
                            },
                            &JsonValue{
                                Kind: &JsonValue_ListValue{
                                    ListValue: &JsonListValue{
                                        Values: []*JsonValue{
                                            &JsonValue{
                                                Kind: &JsonValue_DoubleValue{
                                                    DoubleValue: 1.123,
                                                },
                                            },
                                            &JsonValue{
                                                Kind: &JsonValue_DoubleValue{
                                                    DoubleValue: 2.345,
                                                },
                                            },
                                        },
                                    },
                                },
                            },
                            &JsonValue{
                                Kind: &JsonValue_StructValue{
                                    StructValue: &JsonStruct{
                                        Fields: map[string]*JsonValue{
                                            "n": &JsonValue{
                                                Kind: &JsonValue_NullValue{
                                                    NullValue: JsonNullValue_NULL_VALUE,
                                                },
                                            },
                                            "s": &JsonValue{
                                                Kind: &JsonValue_StringValue{
                                                    StringValue: "test_str",
                                                },
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    }

    structJson = "{\"b\":true,\"i\":100,\"struct\":{\"d\":2.345,\"f\":1.123,\"l\":1001,\"n\":null,\"s\":\"test_str\"}}"
    structMessage = JsonStruct{
        Fields: map[string]*JsonValue{
            "b": &JsonValue{
                Kind: &JsonValue_BoolValue{
                    BoolValue: true,
                },
            },
            "i": &JsonValue{
                Kind: &JsonValue_LongValue{
                    LongValue: 100,
                },
            },
            "struct": &JsonValue{
                Kind: &JsonValue_StructValue{
                    StructValue: &JsonStruct{
                        Fields: map[string]*JsonValue{
                            "l": &JsonValue{
                                Kind: &JsonValue_LongValue{
                                    LongValue: 1001,
                                },
                            },
                            "f": &JsonValue{
                                Kind: &JsonValue_DoubleValue{
                                    DoubleValue: 1.123,
                                },
                            },
                            "d": &JsonValue{
                                Kind: &JsonValue_DoubleValue{
                                    DoubleValue: 2.345,
                                },
                            },
                            "n": &JsonValue{
                                Kind: &JsonValue_NullValue{
                                    NullValue: JsonNullValue_NULL_VALUE,
                                },
                            },
                            "s": &JsonValue{
                                Kind: &JsonValue_StringValue{
                                    StringValue: "test_str",
                                },
                            },
                        },
                    },
                },
            },
        },
    }

    complexStructJson = "{\"b\":true,\"i\":100,\"struct\":{\"l\":1001,\"list\":[1.123,2.345],\"struct\":{\"n\":null,\"s\":\"test_str\"}}}"
    complexStructMessage = JsonStruct{
        Fields: map[string]*JsonValue{
            "b": &JsonValue{
                Kind: &JsonValue_BoolValue{
                    BoolValue: true,
                },
            },
            "i": &JsonValue{
                Kind: &JsonValue_LongValue{
                    LongValue: 100,
                },
            },
            "struct": &JsonValue{
                Kind: &JsonValue_StructValue{
                    StructValue: &JsonStruct{
                        Fields: map[string]*JsonValue{
                            "l": &JsonValue{
                                Kind: &JsonValue_LongValue{
                                    LongValue: 1001,
                                },
                            },
                            "list": &JsonValue{
                                Kind: &JsonValue_ListValue{
                                    ListValue: &JsonListValue{
                                        Values: []*JsonValue{
                                            &JsonValue{
                                                Kind: &JsonValue_DoubleValue{
                                                    DoubleValue: 1.123,
                                                },
                                            },
                                            &JsonValue{
                                                Kind: &JsonValue_DoubleValue{
                                                    DoubleValue: 2.345,
                                                },
                                            },
                                        },
                                    },
                                },
                            },
                            "struct": &JsonValue{
                                Kind: &JsonValue_StructValue{
                                    StructValue: &JsonStruct{
                                        Fields: map[string]*JsonValue{
                                            "n": &JsonValue{
                                                Kind: &JsonValue_NullValue{
                                                    NullValue: JsonNullValue_NULL_VALUE,
                                                },
                                            },
                                            "s": &JsonValue{
                                                Kind: &JsonValue_StringValue{
                                                    StringValue: "test_str",
                                                },
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    }

    emptyListJson = "{\"b\":true,\"i\":100,\"list\":[]}"
    emptyListMessage = JsonStruct{
        Fields: map[string]*JsonValue{
            "b": &JsonValue{
                Kind: &JsonValue_BoolValue{
                    BoolValue: true,
                },
            },
            "i": &JsonValue{
                Kind: &JsonValue_LongValue{
                    LongValue: 100,
                },
            },
            "list": &JsonValue{
                Kind: &JsonValue_ListValue{
                    ListValue: &JsonListValue{
                        Values: []*JsonValue{
                        },
                    },
                },
            },
        },
    }

    emptyStructJson = "{\"b\":true,\"i\":100,\"struct\":{}}"
    emptyStructMessage = JsonStruct{
        Fields: map[string]*JsonValue{
            "b": &JsonValue{
                Kind: &JsonValue_BoolValue{
                    BoolValue: true,
                },
            },
            "i": &JsonValue{
                Kind: &JsonValue_LongValue{
                    LongValue: 100,
                },
            },
            "struct": &JsonValue{
                Kind: &JsonValue_StructValue{
                    StructValue: &JsonStruct{
                        Fields: map[string]*JsonValue{
                        },
                    },
                },
            },
        },
    }

    nestedJson = "{\"lang\":\"test_lang\",\"source\":\"test_source\",\"params\":{\"b\":true,\"i\":100,\"struct\":{\"l\":1001,\"list\":[1.123,2.345],\"struct\":{\"n\":null,\"s\":\"test_str\"}}}}"
    nestedMessage = Script{
        Lang: "test_lang",
        Source: "test_source",
        Params: &complexStructMessage,
    }
)

func AssertEqual(t *testing.T, a interface{}, b interface{}) {
	if a == b {
		return
	}
	t.Errorf("Received %v, expected %v", b, a)
}

func VerifyUnmarshal(t *testing.T, json string, expectedMessage *JsonStruct) {
    var m JsonStruct
    err := jsonpb.UnmarshalString(json, &m)
    if err != nil {
        t.Error(err)
    }
    fmt.Printf("fields: %v\n", m.Fields)
    AssertEqual(t, fmt.Sprintf("%v", expectedMessage.Fields), fmt.Sprintf("%v", m.Fields))
}

func VerifyMarshal(t * testing.T, message *JsonStruct, expectedJson string) {
    m := &jsonpb.Marshaler{}
    jsonStr, err := m.MarshalToString(message)
    if err != nil {
        t.Errorf("Error on marshal")
    }
    fmt.Printf("json: %s\n", jsonStr)
    AssertEqual(t, expectedJson, jsonStr)
}

func VerifyMarshaling(t *testing.T, message *JsonStruct, json string) {
    VerifyUnmarshal(t, json, message)
    VerifyMarshal(t, message, json)
}

func TestEmpty(t *testing.T) {
    VerifyMarshaling(t, &emptyMessage, emptyJson)
}

func TestSimple(t *testing.T) {
    VerifyMarshaling(t, &simpleMessage, simpleJson)
}

func TestList(t *testing.T) {
    VerifyMarshaling(t, &listMessage, listJson)
}

func TestComplexList(t *testing.T) {
    VerifyMarshaling(t, &complexListMessage, complexListJson)
}

func TestStruct(t *testing.T) {
    VerifyMarshaling(t, &structMessage, structJson)
}

func TestComplexStruct(t *testing.T) {
    VerifyMarshaling(t, &complexStructMessage, complexStructJson)
}

func TestEmptyList(t *testing.T) {
    VerifyMarshaling(t, &emptyListMessage, emptyListJson)
}

func TestEmptyStruct(t *testing.T) {
    VerifyMarshaling(t, &emptyStructMessage, emptyStructJson)
}

func TestNested(t *testing.T) {
    m := &jsonpb.Marshaler{}
    jsonStr, err := m.MarshalToString(&nestedMessage)
    if err != nil {
        t.Errorf("Error on marshal")
    }
    fmt.Printf("json: %s\n", jsonStr)
    AssertEqual(t, nestedJson, jsonStr)

    var s Script
    err = jsonpb.UnmarshalString(nestedJson, &s)
    if err != nil {
        t.Error(err)
    }
    AssertEqual(t, fmt.Sprintf("%v", nestedMessage.Lang), fmt.Sprintf("%v", s.Lang))
    AssertEqual(t, fmt.Sprintf("%v", nestedMessage.Source), fmt.Sprintf("%v", s.Source))
    AssertEqual(t, fmt.Sprintf("%v", nestedMessage.Params.Fields), fmt.Sprintf("%v", s.Params.Fields))
}
