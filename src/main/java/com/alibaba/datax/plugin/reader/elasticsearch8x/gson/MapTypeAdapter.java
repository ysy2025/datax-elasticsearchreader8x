package com.alibaba.datax.plugin.reader.elasticsearch8x.gson;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.internal.bind.ObjectTypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author kesc
 * @date 2020-10-13 16:09
 */
public class MapTypeAdapter extends TypeAdapter<Object> {
    public static final TypeAdapterFactory FACTORY = new TypeAdapterFactory() {
        @SuppressWarnings("unchecked")
        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            if (type.getRawType() == Map.class) {
                return (TypeAdapter<T>) new MapTypeAdapter(gson);
            }
            return null;
        }
    };

    private final Gson gson;

    MapTypeAdapter(Gson gson) {
        this.gson = gson;
    }

    @Override
    public Object read(JsonReader in) throws IOException {
        JsonToken token = in.peek();
        switch (token) {
            case BEGIN_ARRAY:
                List<Object> list = new ArrayList<>();
                in.beginArray();
                while (in.hasNext()) {
                    list.add(read(in));
                }
                in.endArray();
                return list;

            case BEGIN_OBJECT:
                Map<String, Object> map = new LinkedTreeMap<>();
                in.beginObject();
                while (in.hasNext()) {
                    map.put(in.nextName(), read(in));
                }
                in.endObject();
                return map;

            case STRING:
                return in.nextString();

            case NUMBER:
                //改写数字的处理逻辑，将数字值分为整型与浮点型
                String numberStr = in.nextString();
                if (numberStr.contains(".") || numberStr.contains("e")
                        || numberStr.contains("E")) {
                    return Double.parseDouble(numberStr);
                }
                /*
                为了避免超大整形数据的bug,提前针对超大整形数据处理;
                利用BigInteger,进行判断
                biginter 不能直接用符号比较,需要使用compareTo方法比较
                int comparisonResult = bigInteger.compareTo(bigIntValue);
                compareTo方法返回一个整数值，
                当bigInteger等于bigIntValue时返回0，
                当bigInteger小于bigIntValue时返回-1，
                当bigInteger大于bigIntValue时返回1。
                 */
                BigInteger bigint = new BigInteger(numberStr);
                BigInteger max = BigInteger.valueOf(Long.MAX_VALUE);
                BigInteger min = BigInteger.valueOf(Long.MIN_VALUE);
                int compareMax = bigint.compareTo(max);
                int compareMin = bigint.compareTo(min);

                // 在极大极小范围内部
                if(compareMin == 1 && compareMax == 0){
                    long value = Long.parseLong(numberStr);
                    if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
                        return (int) value;
                    }
                    return value;
                } else {
                    // 超过了极值
                    return numberStr;
                }


            case BOOLEAN:
                return in.nextBoolean();

            case NULL:
                in.nextNull();
                return null;

            default:
                throw new IllegalStateException();
        }
    }


    @Override
    public void write(JsonWriter out, Object value) throws IOException {
        if (value == null) {
            out.nullValue();
            return;
        }

        TypeAdapter<Object> typeAdapter = gson.getAdapter((Class<Object>) value.getClass());
        if (typeAdapter instanceof ObjectTypeAdapter) {
            out.beginObject();
            out.endObject();
            return;
        }

        typeAdapter.write(out, value);
    }
}