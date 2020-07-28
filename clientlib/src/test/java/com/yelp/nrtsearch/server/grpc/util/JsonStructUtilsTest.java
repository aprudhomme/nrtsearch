package com.yelp.nrtsearch.server.grpc.util;

import com.yelp.nrtsearch.server.grpc.JsonListValue;
import com.yelp.nrtsearch.server.grpc.JsonNullValue;
import com.yelp.nrtsearch.server.grpc.JsonStruct;
import com.yelp.nrtsearch.server.grpc.JsonValue;
import org.junit.Test;

import java.util.*;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class JsonStructUtilsTest {

    @Test( expected = NullPointerException.class)
    public void testEncodeNullStruct() {
        JsonStructUtils.encodeStruct(null);
    }

    @Test
    public void testEncodeEmptyStruct() {
        JsonStruct struct = JsonStructUtils.encodeStruct(Collections.emptyMap());
        assertEquals(struct.getFieldsMap().size(), 0);
    }

    @Test
    public void testEncodeSimpleItems() {
        Map<String, Object> map = new HashMap<>();
        map.put("s", "test_str");
        map.put("b", true);
        map.put("i", 100);
        map.put("l", 1001L);
        map.put("f", 1.123F);
        map.put("d", 2.345);
        map.put("n", null);

        JsonStruct struct = JsonStructUtils.encodeStruct(map);
        assertEquals(7, struct.getFieldsCount());
        assertEquals("test_str", struct.getFieldsOrThrow("s").getStringValue());
        assertTrue(struct.getFieldsOrThrow("b").getBoolValue());
        assertEquals(100, (int) struct.getFieldsOrThrow("i").getLongValue());
        assertEquals(1001L, struct.getFieldsOrThrow("l").getLongValue());
        assertEquals(1.123F, (float) struct.getFieldsOrThrow("f").getDoubleValue(), Math.ulp(1.123F));
        assertEquals(2.345, struct.getFieldsOrThrow("d").getDoubleValue(), Math.ulp(2.345));
        assertEquals(JsonNullValue.NULL_VALUE, struct.getFieldsOrThrow("n").getNullValue());
    }

    @Test
    public void testEncodeList() {
        Map<String, Object> map = new HashMap<>();
        map.put("s", "test_str");
        map.put("b", true);
        List<Object> list = new ArrayList<>();
        list.add(100);
        list.add(1001L);
        list.add(1.123F);
        list.add(2.345);
        list.add(null);
        map.put("list", list);

        JsonStruct struct = JsonStructUtils.encodeStruct(map);
        assertEquals(3, struct.getFieldsCount());
        assertEquals("test_str", struct.getFieldsOrThrow("s").getStringValue());
        assertTrue(struct.getFieldsOrThrow("b").getBoolValue());

        JsonListValue listValue = struct.getFieldsOrThrow("list").getListValue();
        assertEquals(5, listValue.getValuesCount());
        assertEquals(100, (int) listValue.getValues(0).getLongValue());
        assertEquals(1001L, listValue.getValues(1).getLongValue());
        assertEquals(1.123F, (float) listValue.getValues(2).getDoubleValue(), Math.ulp(1.123F));
        assertEquals(2.345, listValue.getValues(3).getDoubleValue(), Math.ulp(2.345));
        assertEquals(JsonNullValue.NULL_VALUE, listValue.getValues(4).getNullValue());
    }

    @Test
    public void testEncodeComplexList() {
        Map<String, Object> map = new HashMap<>();
        map.put("s", "test_str");
        map.put("b", true);
        List<Object> list = new ArrayList<>();
        list.add(100);
        List<Object> list2 = new ArrayList<>();
        list2.add(1001L);
        list2.add(1.123F);
        list.add(list2);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("d", 2.345);
        map2.put("n", null);
        list.add(map2);
        map.put("list", list);

        JsonStruct struct = JsonStructUtils.encodeStruct(map);
        assertEquals(3, struct.getFieldsCount());
        assertEquals("test_str", struct.getFieldsOrThrow("s").getStringValue());
        assertTrue(struct.getFieldsOrThrow("b").getBoolValue());

        JsonListValue listValue = struct.getFieldsOrThrow("list").getListValue();
        assertEquals(3, listValue.getValuesCount());
        assertEquals(100, (int) listValue.getValues(0).getLongValue());

        JsonListValue listValue2 = listValue.getValues(1).getListValue();
        assertEquals(2, listValue2.getValuesCount());
        assertEquals(1001L, listValue2.getValues(0).getLongValue());
        assertEquals(1.123F, (float) listValue2.getValues(1).getDoubleValue(), Math.ulp(1.123F));

        JsonStruct struct2 = listValue.getValues(2).getStructValue();
        assertEquals(2, struct2.getFieldsCount());
        assertEquals(2.345, struct2.getFieldsOrThrow("d").getDoubleValue(), Math.ulp(2.345));
        assertEquals(JsonNullValue.NULL_VALUE, struct2.getFieldsOrThrow("n").getNullValue());
    }

    @Test
    public void testEncodeStruct() {
        Map<String, Object> map = new HashMap<>();
        map.put("s", "test_str");
        map.put("b", true);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("i", 100);
        map2.put("l", 1001L);
        map2.put("f", 1.123F);
        map2.put("d", 2.345);
        map2.put("n", null);
        map.put("struct", map2);

        JsonStruct struct = JsonStructUtils.encodeStruct(map);
        assertEquals(3, struct.getFieldsCount());
        assertEquals("test_str", struct.getFieldsOrThrow("s").getStringValue());
        assertTrue(struct.getFieldsOrThrow("b").getBoolValue());

        JsonStruct struct2 = struct.getFieldsOrThrow("struct").getStructValue();
        assertEquals(5, struct2.getFieldsCount());
        assertEquals(100, (int) struct2.getFieldsOrThrow("i").getLongValue());
        assertEquals(1001L, struct2.getFieldsOrThrow("l").getLongValue());
        assertEquals(1.123F, (float) struct2.getFieldsOrThrow("f").getDoubleValue(), Math.ulp(1.123F));
        assertEquals(2.345, struct2.getFieldsOrThrow("d").getDoubleValue(), Math.ulp(2.345));
        assertEquals(JsonNullValue.NULL_VALUE, struct2.getFieldsOrThrow("n").getNullValue());
    }

    @Test
    public void testEncodeComplexStruct() {
        Map<String, Object> map = new HashMap<>();
        map.put("s", "test_str");
        map.put("b", true);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("i", 100);
        List<Object> list = new ArrayList<>();
        list.add(1001L);
        list.add(1.123F);
        map2.put("list", list);
        Map<String, Object> map3 = new HashMap<>();
        map3.put("d", 2.345);
        map3.put("n", null);
        map2.put("struct", map3);
        map.put("struct", map2);

        JsonStruct struct = JsonStructUtils.encodeStruct(map);
        assertEquals(3, struct.getFieldsCount());
        assertEquals("test_str", struct.getFieldsOrThrow("s").getStringValue());
        assertTrue(struct.getFieldsOrThrow("b").getBoolValue());

        JsonStruct struct2 = struct.getFieldsOrThrow("struct").getStructValue();
        assertEquals(3, struct2.getFieldsCount());
        assertEquals(100, (int) struct2.getFieldsOrThrow("i").getLongValue());

        JsonListValue listValue = struct2.getFieldsOrThrow("list").getListValue();
        assertEquals(2, listValue.getValuesCount());
        assertEquals(1001L, listValue.getValues(0).getLongValue());
        assertEquals(1.123F, (float) listValue.getValues(1).getDoubleValue(), Math.ulp(1.123F));

        JsonStruct struct3 = struct2.getFieldsOrThrow("struct").getStructValue();
        assertEquals(2, struct3.getFieldsCount());
        assertEquals(2.345, struct3.getFieldsOrThrow("d").getDoubleValue(), Math.ulp(2.345));
        assertEquals(JsonNullValue.NULL_VALUE, struct3.getFieldsOrThrow("n").getNullValue());
    }

    @Test
    public void testEncodeEmptyListItem() {
        Map<String, Object> map = new HashMap<>();
        map.put("s", "test_str");
        map.put("b", true);
        List<Object> list = new ArrayList<>();
        map.put("list", list);

        JsonStruct struct = JsonStructUtils.encodeStruct(map);
        assertEquals(3, struct.getFieldsCount());
        assertEquals("test_str", struct.getFieldsOrThrow("s").getStringValue());
        assertTrue(struct.getFieldsOrThrow("b").getBoolValue());

        JsonListValue listValue = struct.getFieldsOrThrow("list").getListValue();
        assertEquals(0, listValue.getValuesCount());
    }

    @Test
    public void testEncodeEmptyStructItem() {
        Map<String, Object> map = new HashMap<>();
        map.put("s", "test_str");
        map.put("b", true);
        Map<String, Object> map2 = new HashMap<>();
        map.put("struct", map2);

        JsonStruct struct = JsonStructUtils.encodeStruct(map);
        assertEquals(3, struct.getFieldsCount());
        assertEquals("test_str", struct.getFieldsOrThrow("s").getStringValue());
        assertTrue(struct.getFieldsOrThrow("b").getBoolValue());

        JsonStruct struct2 = struct.getFieldsOrThrow("struct").getStructValue();
        assertEquals(0, struct2.getFieldsCount());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEncodeInvalidMapKeyType() {
        Map<String, Object> map = new HashMap<>();
        map.put("s", "test_str");
        map.put("b", true);
        Map<Long, Object> map2 = new HashMap<>();
        map2.put(2L, 1L);
        map.put("struct", map2);

        JsonStructUtils.encodeStruct(map);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEncodeNullMapKey() {
        Map<String, Object> map = new HashMap<>();
        map.put("s", "test_str");
        map.put("b", true);
        Map<String, Object> map2 = new HashMap<>();
        map2.put(null, 1L);
        map.put("struct", map2);

        JsonStructUtils.encodeStruct(map);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEncodeInvalidObject() {
        Map<String, Object> map = new HashMap<>();
        map.put("s", "test_str");
        map.put("b", true);
        map.put("invalid", new Object());

        JsonStructUtils.encodeStruct(map);
    }

    @Test(expected = NullPointerException.class)
    public void testDecodeNullStructCopy() {
        JsonStructUtils.decodeStructCopy(null);
    }

    @Test(expected = NullPointerException.class)
    public void testDecodeNullStructLazy() {
        JsonStructUtils.decodeStructLazy(null);
    }

    @Test
    public void testDecodeEmptyStruct() {
        JsonStruct struct = JsonStruct.newBuilder().build();

        Consumer<Map<String,?>> assertFunc = m -> assertEquals(0, m.size());

        assertFunc.accept(JsonStructUtils.decodeStructCopy(struct));
        assertFunc.accept(JsonStructUtils.decodeStructLazy(struct));
    }

    @Test
    public void testDecodeSimpleItems() {
        JsonStruct struct = JsonStruct.newBuilder()
                .putFields("s", JsonValue.newBuilder()
                        .setStringValue("test_str")
                        .build())
                .putFields("b", JsonValue.newBuilder()
                        .setBoolValue(true)
                        .build())
                .putFields("i", JsonValue.newBuilder()
                        .setLongValue(100)
                        .build())
                .putFields("l", JsonValue.newBuilder()
                        .setLongValue(1001L)
                        .build())
                .putFields("f", JsonValue.newBuilder()
                        .setDoubleValue(1.123F)
                        .build())
                .putFields("d", JsonValue.newBuilder()
                        .setDoubleValue(2.345)
                        .build())
                .putFields("n", JsonValue.newBuilder()
                        .setNullValue(JsonNullValue.NULL_VALUE)
                        .build())
                .build();

        Consumer<Map<String,?>> assertFunc = m -> {
            assertEquals(7, m.size());
            assertEquals("test_str", m.get("s"));
            assertTrue((Boolean) m.get("b"));
            assertEquals(100, ((Long) m.get("i")).intValue());
            assertEquals(1001L, ((Long) m.get("l")).longValue());
            assertEquals(1.123F, ((Double) m.get("f")).floatValue(), Math.ulp(1.123F));
            assertEquals(2.345, (Double) m.get("d"), Math.ulp(2.345));
            assertNull(m.get("n"));
        };

        assertFunc.accept(JsonStructUtils.decodeStructCopy(struct));
        assertFunc.accept(JsonStructUtils.decodeStructLazy(struct));
    }

    @Test
    public void testDecodeList() {
        JsonStruct struct = JsonStruct.newBuilder()
                .putFields("s", JsonValue.newBuilder()
                        .setStringValue("test_str")
                        .build())
                .putFields("b", JsonValue.newBuilder()
                        .setBoolValue(true)
                        .build())
                .putFields("list", JsonValue.newBuilder()
                        .setListValue(JsonListValue.newBuilder()
                                .addValues(JsonValue.newBuilder()
                                        .setLongValue(100)
                                        .build())
                                .addValues(JsonValue.newBuilder()
                                        .setLongValue(1001L)
                                        .build())
                                .addValues(JsonValue.newBuilder()
                                        .setDoubleValue(1.123F)
                                        .build())
                                .addValues(JsonValue.newBuilder()
                                        .setDoubleValue(2.345)
                                        .build())
                                .addValues(JsonValue.newBuilder()
                                        .setNullValue(JsonNullValue.NULL_VALUE)
                                        .build())
                                .build())
                        .build())
                .build();

        Consumer<Map<String,?>> assertFunc = m -> {
            assertEquals(3, m.size());
            assertEquals("test_str", m.get("s"));
            assertTrue((Boolean) m.get("b"));

            List<?> list = (List<?>) m.get("list");
            assertEquals(5, list.size());
            assertEquals(100, ((Long) list.get(0)).intValue());
            assertEquals(1001L, ((Long) list.get(1)).longValue());
            assertEquals(1.123F, ((Double) list.get(2)).floatValue(), Math.ulp(1.123F));
            assertEquals(2.345, (Double) list.get(3), Math.ulp(2.345));
            assertNull(list.get(4));
        };

        assertFunc.accept(JsonStructUtils.decodeStructCopy(struct));
        assertFunc.accept(JsonStructUtils.decodeStructLazy(struct));
    }

    @Test
    public void testDecodeComplexList() {
        JsonStruct struct = JsonStruct.newBuilder()
                .putFields("s", JsonValue.newBuilder()
                        .setStringValue("test_str")
                        .build())
                .putFields("b", JsonValue.newBuilder()
                        .setBoolValue(true)
                        .build())
                .putFields("list", JsonValue.newBuilder()
                        .setListValue(JsonListValue.newBuilder()
                                .addValues(JsonValue.newBuilder()
                                        .setLongValue(100)
                                        .build())
                                .addValues(JsonValue.newBuilder()
                                        .setListValue(JsonListValue.newBuilder()
                                                .addValues(JsonValue.newBuilder()
                                                        .setLongValue(1001L)
                                                        .build())
                                                .addValues(JsonValue.newBuilder()
                                                        .setDoubleValue(1.123F)
                                                        .build())
                                                .build())
                                        .build())
                                .addValues(JsonValue.newBuilder()
                                        .setStructValue(JsonStruct.newBuilder()
                                                .putFields("d", JsonValue.newBuilder()
                                                        .setDoubleValue(2.345)
                                                        .build())
                                                .putFields("n", JsonValue.newBuilder()
                                                        .setNullValue(JsonNullValue.NULL_VALUE)
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        Consumer<Map<String,?>> assertFunc = m -> {
            assertEquals(3, m.size());
            assertEquals("test_str", m.get("s"));
            assertTrue((Boolean) m.get("b"));

            List<?> list = (List<?>) m.get("list");
            assertEquals(3, list.size());
            assertEquals(100, ((Long) list.get(0)).intValue());

            List<?> list2 = (List<?>) list.get(1);
            assertEquals(2, list2.size());
            assertEquals(1001L, ((Long) list2.get(0)).longValue());
            assertEquals(1.123F, ((Double) list2.get(1)).floatValue(), Math.ulp(1.123F));

            @SuppressWarnings("unchecked")
            Map<String, ?> m2 = (Map<String, ?>) list.get(2);
            assertEquals(2, m2.size());
            assertEquals(2.345, (Double) m2.get("d"), Math.ulp(2.345));
            assertNull(m2.get("n"));
        };

        assertFunc.accept(JsonStructUtils.decodeStructCopy(struct));
        assertFunc.accept(JsonStructUtils.decodeStructLazy(struct));
    }

    @Test
    public void testDecodeStruct() {
        JsonStruct struct = JsonStruct.newBuilder()
                .putFields("s", JsonValue.newBuilder()
                        .setStringValue("test_str")
                        .build())
                .putFields("b", JsonValue.newBuilder()
                        .setBoolValue(true)
                        .build())
                .putFields("struct", JsonValue.newBuilder()
                        .setStructValue(JsonStruct.newBuilder()
                                .putFields("i", JsonValue.newBuilder()
                                        .setLongValue(100)
                                        .build())
                                .putFields("l", JsonValue.newBuilder()
                                        .setLongValue(1001L)
                                        .build())
                                .putFields("f", JsonValue.newBuilder()
                                        .setDoubleValue(1.123F)
                                        .build())
                                .putFields("d", JsonValue.newBuilder()
                                        .setDoubleValue(2.345)
                                        .build())
                                .putFields("n", JsonValue.newBuilder()
                                        .setNullValue(JsonNullValue.NULL_VALUE)
                                        .build())
                                .build())
                        .build())
                .build();

        Consumer<Map<String,?>> assertFunc = m -> {
            assertEquals(3, m.size());
            assertEquals("test_str", m.get("s"));
            assertTrue((Boolean) m.get("b"));

            @SuppressWarnings("unchecked")
            Map<String, ?> m2 = (Map<String, ?>) m.get("struct");
            assertEquals(5, m2.size());
            assertEquals(100, ((Long) m2.get("i")).intValue());
            assertEquals(1001L, ((Long) m2.get("l")).longValue());
            assertEquals(1.123F, ((Double) m2.get("f")).floatValue(), Math.ulp(1.123F));
            assertEquals(2.345, (Double) m2.get("d"), Math.ulp(2.345));
            assertNull(m2.get("n"));
        };

        assertFunc.accept(JsonStructUtils.decodeStructCopy(struct));
        assertFunc.accept(JsonStructUtils.decodeStructLazy(struct));
    }

    @Test
    public void testDecodeComplexStruct() {
        JsonStruct struct = JsonStruct.newBuilder()
                .putFields("s", JsonValue.newBuilder()
                        .setStringValue("test_str")
                        .build())
                .putFields("b", JsonValue.newBuilder()
                        .setBoolValue(true)
                        .build())
                .putFields("struct", JsonValue.newBuilder()
                        .setStructValue(JsonStruct.newBuilder()
                                .putFields("i", JsonValue.newBuilder()
                                        .setLongValue(100)
                                        .build())
                                .putFields("list", JsonValue.newBuilder()
                                        .setListValue(JsonListValue.newBuilder()
                                                .addValues(JsonValue.newBuilder()
                                                        .setLongValue(1001L)
                                                        .build())
                                                .addValues(JsonValue.newBuilder()
                                                        .setDoubleValue(1.123F)
                                                        .build())
                                                .build())
                                        .build())
                                .putFields("struct", JsonValue.newBuilder()
                                        .setStructValue(JsonStruct.newBuilder()
                                                .putFields("d", JsonValue.newBuilder()
                                                        .setDoubleValue(2.345)
                                                        .build())
                                                .putFields("n", JsonValue.newBuilder()
                                                        .setNullValue(JsonNullValue.NULL_VALUE)
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        Consumer<Map<String,?>> assertFunc = m -> {
            assertEquals(3, m.size());
            assertEquals("test_str", m.get("s"));
            assertTrue((Boolean) m.get("b"));

            @SuppressWarnings("unchecked")
            Map<String, ?> m2 = (Map<String, ?>) m.get("struct");
            assertEquals(3, m2.size());
            assertEquals(100, ((Long) m2.get("i")).intValue());

            List<?> list = (List<?>) m2.get("list");
            assertEquals(2, list.size());
            assertEquals(1001L, ((Long) list.get(0)).longValue());
            assertEquals(1.123F, ((Double) list.get(1)).floatValue(), Math.ulp(1.123F));

            @SuppressWarnings("unchecked")
            Map<String, ?> m3 = (Map<String, ?>) m2.get("struct");
            assertEquals(2, m3.size());
            assertEquals(2.345, (Double) m3.get("d"), Math.ulp(2.345));
            assertNull(m3.get("n"));
        };

        assertFunc.accept(JsonStructUtils.decodeStructCopy(struct));
        assertFunc.accept(JsonStructUtils.decodeStructLazy(struct));
    }

    @Test
    public void testDecodeEmptyListItem() {
        JsonStruct struct = JsonStruct.newBuilder()
                .putFields("s", JsonValue.newBuilder()
                        .setStringValue("test_str")
                        .build())
                .putFields("b", JsonValue.newBuilder()
                        .setBoolValue(true)
                        .build())
                .putFields("list", JsonValue.newBuilder()
                        .setListValue(JsonListValue.newBuilder().build())
                        .build())
                .build();

        Consumer<Map<String,?>> assertFunc = m -> {
            assertEquals(3, m.size());
            assertEquals("test_str", m.get("s"));
            assertTrue((Boolean) m.get("b"));

            List<?> list = (List<?>) m.get("list");
            assertEquals(0, list.size());
        };

        assertFunc.accept(JsonStructUtils.decodeStructCopy(struct));
        assertFunc.accept(JsonStructUtils.decodeStructLazy(struct));
    }

    @Test
    public void testDecodeEmptyStructItem() {
        JsonStruct struct = JsonStruct.newBuilder()
                .putFields("s", JsonValue.newBuilder()
                        .setStringValue("test_str")
                        .build())
                .putFields("b", JsonValue.newBuilder()
                        .setBoolValue(true)
                        .build())
                .putFields("struct", JsonValue.newBuilder()
                        .setStructValue(JsonStruct.newBuilder().build())
                        .build())
                .build();

        Consumer<Map<String,?>> assertFunc = m -> {
            assertEquals(3, m.size());
            assertEquals("test_str", m.get("s"));
            assertTrue((Boolean) m.get("b"));

            @SuppressWarnings("unchecked")
            Map<String, ?> m2 = (Map<String, ?>) m.get("struct");
            assertEquals(0, m2.size());
        };

        assertFunc.accept(JsonStructUtils.decodeStructCopy(struct));
        assertFunc.accept(JsonStructUtils.decodeStructLazy(struct));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeUnsetValueCopy() {
        JsonStruct struct = JsonStruct.newBuilder()
                .putFields("s", JsonValue.newBuilder()
                        .setStringValue("test_str")
                        .build())
                .putFields("b", JsonValue.newBuilder()
                        .setBoolValue(true)
                        .build())
                .putFields("struct", JsonValue.newBuilder()
                        .build())
                .build();
        JsonStructUtils.decodeStructCopy(struct);
    }

    @Test
    public void testDecodeUnsetValueLazy() {
        JsonStruct struct = JsonStruct.newBuilder()
                .putFields("s", JsonValue.newBuilder()
                        .setStringValue("test_str")
                        .build())
                .putFields("b", JsonValue.newBuilder()
                        .build())
                .build();
        JsonStructUtils.decodeStructLazy(struct);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeUnsetValueLazyThrow() {
        JsonStruct struct = JsonStruct.newBuilder()
                .putFields("s", JsonValue.newBuilder()
                        .setStringValue("test_str")
                        .build())
                .putFields("b", JsonValue.newBuilder()
                        .build())
                .build();
        Map<String, ?> m = JsonStructUtils.decodeStructLazy(struct);
        m.get("b");
    }
}
