package luceneserver;

import (
    "encoding/json"
    "errors"
    "fmt"
    "math"
    "sort"
    "strconv"
    "strings"

    "github.com/golang/protobuf/jsonpb"
    "google.golang.org/protobuf/proto"
    "google.golang.org/protobuf/reflect/protoreflect"
)

func (js *JsonStruct) UnmarshalJSONPB(u *jsonpb.Unmarshaler, raw []byte) error {
    mr := js.ProtoReflect()

    if string(raw) == "null" {
    	return nil
    }

    if err := js.unmarshalMessage(mr, raw); err != nil {
    	return err
    }
    return proto.CheckInitialized(mr.Interface())
}

func (u *JsonStruct) unmarshalMessage(m protoreflect.Message, in []byte) error {
	md := m.Descriptor()
	fds := md.Fields()
	name := md.Name()

	if string(in) == "null" && md.Name() != "JsonValue" {
    	return nil
    }

    switch name {
        case "JsonValue":
        	switch {
        		case string(in) == "null":
        			m.Set(fds.ByNumber(1), protoreflect.ValueOfEnum(0))
        		case string(in) == "true":
        			m.Set(fds.ByNumber(5), protoreflect.ValueOfBool(true))
        		case string(in) == "false":
        			m.Set(fds.ByNumber(5), protoreflect.ValueOfBool(false))
        		case hasPrefixAndSuffix('"', in, '"'):
        			s, err := unquoteString(string(in))
        			if err != nil {
        				return fmt.Errorf("unrecognized type for Value %q", in)
        			}
        			m.Set(fds.ByNumber(4), protoreflect.ValueOfString(s))
        		case hasPrefixAndSuffix('[', in, ']'):
        			v := m.Mutable(fds.ByNumber(7))
        			return u.unmarshalMessage(v.Message(), in)
        		case hasPrefixAndSuffix('{', in, '}'):
        			v := m.Mutable(fds.ByNumber(6))
        			return u.unmarshalMessage(v.Message(), in)
        		default:
        		    valueStr := string(in)
        		    if (!strings.Contains(valueStr, ".")) {
        		        i, err := strconv.ParseInt(valueStr, 0, 64)
        		        if err == nil {
        			        m.Set(fds.ByNumber(3), protoreflect.ValueOfInt64(i))
        			        return nil
        		        }
        		    }
        			f, err := strconv.ParseFloat(valueStr, 0)
        			if err != nil {
        				return fmt.Errorf("unrecognized type for Value %q", in)
        			}
        			m.Set(fds.ByNumber(2), protoreflect.ValueOfFloat64(f))
        	}
        	return nil
        case "JsonListValue":
        	var jsonArray []json.RawMessage
        	if err := json.Unmarshal(in, &jsonArray); err != nil {
        		return fmt.Errorf("bad JsonListValue: %v", err)
        	}

        	lv := m.Mutable(fds.ByNumber(1)).List()
        	for _, raw := range jsonArray {
        		ve := lv.NewElement()
        		if err := u.unmarshalMessage(ve.Message(), raw); err != nil {
        				return err
        		}
        		lv.Append(ve)
        	}
        	return nil
        case "JsonStruct":
            var jsonObject map[string]json.RawMessage
            if err := json.Unmarshal(in, &jsonObject); err != nil {
        	    return fmt.Errorf("bad JsonStructValue: %v", err)
            }

            mv := m.Mutable(fds.ByNumber(1)).Map()
            for key, raw := range jsonObject {
        		kv := protoreflect.ValueOf(key).MapKey()
        		vv := mv.NewValue()
        		if err := u.unmarshalMessage(vv.Message(), raw); err != nil {
        			return fmt.Errorf("bad value in StructValue for key %q: %v", key, err)
        		}
        		mv.Set(kv, vv)
        	}
        	return nil
        default:
            return fmt.Errorf("Unknown message name: %s", name)
    }
}

func unquoteString(in string) (out string, err error) {
	err = json.Unmarshal([]byte(in), &out)
	return out, err
}

func hasPrefixAndSuffix(prefix byte, in []byte, suffix byte) bool {
	if len(in) >= 2 && in[0] == prefix && in[len(in)-1] == suffix {
		return true
	}
	return false
}

func (js *JsonStruct) MarshalJSONPB(m *jsonpb.Marshaler) ([]byte, error) {
    m2 := js.ProtoReflect()
    if err := proto.CheckInitialized(m2.Interface()); err != nil {
    	return nil, err
    }

    w := jsonWriter{Marshaler: m}
    err := w.marshalMessage(m2, "", "")
    return w.buf, err
}

type jsonWriter struct {
	*jsonpb.Marshaler
	buf []byte
}

func (w *jsonWriter) write(s string) {
	w.buf = append(w.buf, s...)
}

func (w *jsonWriter) marshalMessage(m protoreflect.Message, indent, typeURL string) error {
    md := m.Descriptor()
    fds := md.Fields()

    switch md.Name() {
        case "JsonStruct", "JsonListValue":
        	// JSON object or array.
        	fd := fds.ByNumber(1)
        	return w.marshalValue(fd, m.Get(fd), indent)
        case "JsonValue":
        	// JSON value; which is a null, number, string, bool, object, or array.
        	od := md.Oneofs().Get(0)
        	fd := m.WhichOneof(od)
        	if fd == nil {
        		return errors.New("nil Value")
        	}
        	return w.marshalValue(fd, m.Get(fd), indent)
    }
    return nil
}

func (w *jsonWriter) marshalValue(fd protoreflect.FieldDescriptor, v protoreflect.Value, indent string) error {
	switch {
	case fd.IsList():
		w.write("[")
		comma := ""
		lv := v.List()
		for i := 0; i < lv.Len(); i++ {
			w.write(comma)
			if w.Indent != "" {
				w.write("\n")
				w.write(indent)
				w.write(w.Indent)
				w.write(w.Indent)
			}
			if err := w.marshalSingularValue(fd, lv.Get(i), indent+w.Indent); err != nil {
				return err
			}
			comma = ","
		}
		if w.Indent != "" {
			w.write("\n")
			w.write(indent)
			w.write(w.Indent)
		}
		w.write("]")
		return nil
	case fd.IsMap():
		kfd := fd.MapKey()
		vfd := fd.MapValue()
		mv := v.Map()

		// Collect a sorted list of all map keys and values.
		type entry struct{ key, val protoreflect.Value }
		var entries []entry
		mv.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			entries = append(entries, entry{k.Value(), v})
			return true
		})
		sort.Slice(entries, func(i, j int) bool {
			switch kfd.Kind() {
			case protoreflect.BoolKind:
				return !entries[i].key.Bool() && entries[j].key.Bool()
			case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind, protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
				return entries[i].key.Int() < entries[j].key.Int()
			case protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
				return entries[i].key.Uint() < entries[j].key.Uint()
			case protoreflect.StringKind:
				return entries[i].key.String() < entries[j].key.String()
			default:
				panic("invalid kind")
			}
		})

		w.write(`{`)
		comma := ""
		for _, entry := range entries {
			w.write(comma)
			if w.Indent != "" {
				w.write("\n")
				w.write(indent)
				w.write(w.Indent)
				w.write(w.Indent)
			}

			s := fmt.Sprint(entry.key.Interface())
			b, err := json.Marshal(s)
			if err != nil {
				return err
			}
			w.write(string(b))

			w.write(`:`)
			if w.Indent != "" {
				w.write(` `)
			}

			if err := w.marshalSingularValue(vfd, entry.val, indent+w.Indent); err != nil {
				return err
			}
			comma = ","
		}
		if w.Indent != "" {
			w.write("\n")
			w.write(indent)
			w.write(w.Indent)
		}
		w.write(`}`)
		return nil
	default:
		return w.marshalSingularValue(fd, v, indent)
	}
}

func (w *jsonWriter) marshalSingularValue(fd protoreflect.FieldDescriptor, v protoreflect.Value, indent string) error {
	switch {
	case !v.IsValid():
		w.write("null")
		return nil
	case fd.Message() != nil:
		return w.marshalMessage(v.Message(), indent+w.Indent, "")
	case fd.Enum() != nil:
		if fd.Enum().Name() == "JsonNullValue" {
			w.write("null")
			return nil
		}

		vd := fd.Enum().Values().ByNumber(v.Enum())
		if vd == nil || w.EnumsAsInts {
			w.write(strconv.Itoa(int(v.Enum())))
		} else {
			w.write(`"` + string(vd.Name()) + `"`)
		}
		return nil
	default:
		switch v.Interface().(type) {
		case float32, float64:
			switch {
			case math.IsInf(v.Float(), +1):
				w.write(`"Infinity"`)
				return nil
			case math.IsInf(v.Float(), -1):
				w.write(`"-Infinity"`)
				return nil
			case math.IsNaN(v.Float()):
				w.write(`"NaN"`)
				return nil
			}
		}

		b, err := json.Marshal(v.Interface())
		if err != nil {
			return err
		}
		w.write(string(b))
		return nil
	}
}