package com.yelp.nrtsearch.server.grpc.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yelp.nrtsearch.server.grpc.JsonListValue;
import com.yelp.nrtsearch.server.grpc.JsonNullValue;
import com.yelp.nrtsearch.server.grpc.JsonStruct;
import com.yelp.nrtsearch.server.grpc.JsonValue;

import java.util.*;
import java.util.function.Function;

/**
 * Utility class for converting between {@link JsonStruct} and {@link Map}.
 */
public class JsonStructUtils {
    private JsonStructUtils() {

    }

    /**
     * Decode a {@link JsonStruct} message into a java native {@link Map} of the
     * same structure. The conversion is done lazily as items are accessed.
     * @param struct json struct message
     * @return java native view of struct message
     * @throws NullPointerException if struct is null
     */
    public static Map<String, Object> decodeStructLazy(JsonStruct struct) {
        Objects.requireNonNull(struct);
        return Maps.transformValues(struct.getFieldsMap(), ScriptParamsLazyTransformer.INSTANCE);
    }

    /**
     * Decode a {@link JsonStruct} message into a java native {@link Map} of the
     * same structure. Builds java native representation up front as a copy of the
     * original.
     * @param struct json struct message
     * @return java native copy of struct message
     * @throws IllegalArgumentException if {@link JsonValue} is not set
     * @throws NullPointerException if struct is null
     */
    public static Map<String, Object> decodeStructCopy(JsonStruct struct) {
        Objects.requireNonNull(struct);
        Map<String, Object> structMap = new HashMap<>();
        for (Map.Entry<String, JsonValue> entry : struct.getFieldsMap().entrySet()) {
            structMap.put(entry.getKey(), ScriptParamsTransformer.INSTANCE.apply(entry.getValue()));
        }
        return structMap;
    }

    /**
     * Encode a native java {@link Map} containing json compatible items into a
     * {@link JsonStruct}. Types must be one of: {@link String}, {@link Boolean},
     * {@link Number}, {@link List}, {@link Map}, null. All Maps must have String keys.
     * @param jsonMap java native representation of a {@link JsonStruct}
     * @return {@link JsonStruct} message built from native {@link Map}
     * @throws IllegalArgumentException if any map key is not a String, or if any
     * item is an unexpected type
     * @throws NullPointerException if jsonMap is null
     */
    public static JsonStruct encodeStruct(Map<String,?> jsonMap) {
        Objects.requireNonNull(jsonMap);

        JsonStruct.Builder structBuilder = JsonStruct.newBuilder();
        for (Map.Entry<String,?> entry : jsonMap.entrySet()) {
            structBuilder.putFields(entry.getKey(), encodeValue(entry.getValue()));
        }
        return structBuilder.build();
    }

    private static JsonValue encodeValue(Object item) {
        if (item == null) {
            return JsonValue.newBuilder()
                    .setNullValue(JsonNullValue.NULL_VALUE)
                    .build();
        } else if (item instanceof Integer || item instanceof Long) {
            return JsonValue.newBuilder()
                    .setLongValue(((Number) item).longValue())
                    .build();
        } else if (item instanceof Number) {
            return JsonValue.newBuilder()
                    .setDoubleValue(((Number) item).doubleValue())
                    .build();
        } else if (item instanceof String) {
            return JsonValue.newBuilder()
                    .setStringValue((String) item)
                    .build();
        } else if (item instanceof Boolean) {
            return JsonValue.newBuilder()
                    .setBoolValue((Boolean) item)
                    .build();
        } else if (item instanceof Map) {
            JsonStruct.Builder structBuilder = JsonStruct.newBuilder();
            Map<?,?> map = (Map<?,?>) item;
            for (Map.Entry<?,?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw new IllegalArgumentException("Invalid json map key: " +
                            entry.getKey() + ", expected type String");
                }
                structBuilder.putFields((String) entry.getKey(), encodeValue(entry.getValue()));
            }
            return JsonValue.newBuilder()
                    .setStructValue(structBuilder.build())
                    .build();
        } else if (item instanceof List) {
            JsonListValue.Builder listBuilder = JsonListValue.newBuilder();
            List<?> list = (List<?>) item;
            for (Object entry : list) {
                listBuilder.addValues(encodeValue(entry));
            }
            return JsonValue.newBuilder()
                    .setListValue(listBuilder.build())
                    .build();
        }
        throw new IllegalArgumentException("Item is not json compatible: " +
                item + ", type: " + item.getClass());
    }

    public static class ScriptParamsLazyTransformer
            implements com.google.common.base.Function<JsonValue, Object>,
            Function<JsonValue, Object> {
        public static final ScriptParamsLazyTransformer INSTANCE = new ScriptParamsLazyTransformer();

        @Override
        public Object apply(JsonValue input) {
            if (input == null) {
                return null;
            }
            switch (input.getKindCase()) {
                case BOOL_VALUE:
                    return input.getBoolValue();
                case LONG_VALUE:
                    return input.getLongValue();
                case DOUBLE_VALUE:
                    return input.getDoubleValue();
                case STRING_VALUE:
                    return input.getStringValue();
                case LIST_VALUE:
                    return Lists.transform(input.getListValue().getValuesList(), INSTANCE);
                case STRUCT_VALUE:
                    return Maps.transformValues(input.getStructValue().getFieldsMap(), INSTANCE);
                case NULL_VALUE:
                    return null;
                default:
                    throw new IllegalArgumentException("Unknown value type: " + input.getKindCase());
            }
        }
    }

    public static class ScriptParamsTransformer
            implements com.google.common.base.Function<JsonValue, Object>,
            Function<JsonValue, Object> {
        public static final ScriptParamsTransformer INSTANCE = new ScriptParamsTransformer();

        @Override
        public Object apply(JsonValue input) {
            if (input == null) {
                return null;
            }
            switch (input.getKindCase()) {
                case BOOL_VALUE:
                    return input.getBoolValue();
                case LONG_VALUE:
                    return input.getLongValue();
                case DOUBLE_VALUE:
                    return input.getDoubleValue();
                case STRING_VALUE:
                    return input.getStringValue();
                case LIST_VALUE:
                    List<Object> list = new ArrayList<>(input.getListValue().getValuesList().size());
                    for (JsonValue entry : input.getListValue().getValuesList()) {
                        list.add(INSTANCE.apply(entry));
                    }
                    return list;
                case STRUCT_VALUE:
                    Map<String, Object> structMap = new HashMap<>();
                    for (Map.Entry<String, JsonValue> entry : input.getStructValue().getFieldsMap().entrySet()) {
                        structMap.put(entry.getKey(), INSTANCE.apply(entry.getValue()));
                    }
                    return structMap;
                case NULL_VALUE:
                    return null;
                default:
                    throw new IllegalArgumentException("Unknown value type: " + input.getKindCase());
            }
        }
    }
}
