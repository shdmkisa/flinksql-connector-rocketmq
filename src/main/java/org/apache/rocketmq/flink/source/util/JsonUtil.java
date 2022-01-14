package org.apache.rocketmq.flink.source.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MapperFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.FlinkRuntimeException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.stream.Collectors.toMap;


public class JsonUtil {

    public static final ObjectMapper objectMapper =
            new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE =
            new TypeReference<Map<String, Object>>() {};

    /**
     * json反序列化成实体对象
     *
     * @param jsonStr json字符串
     * @param clazz 实体类class
     * @param <T> 泛型
     * @return 实体对象
     */
    public static <T> T toObject(String jsonStr, Class<T> clazz) {
        try {
            return objectMapper.readValue(jsonStr, clazz);
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "error parse [" + jsonStr + "] to [" + clazz.getName() + "]", e);
        }
    }

    /**
     * json反序列化成实体对象
     *
     * @param jsonStr json字符串
     * @param <T> 泛型
     * @return 实体对象
     */
    public static <T> T toObject(String jsonStr, TypeReference<T> valueTypeRef) {
        try {
            return objectMapper.readValue(jsonStr, valueTypeRef);
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "error parse ["
                            + jsonStr
                            + "] to ["
                            + valueTypeRef.getType().getTypeName()
                            + "]",
                    e);
        }
    }

    /**
     * 实体对象转json字符串
     *
     * @param obj 实体对象
     * @return json字符串
     */
    public static String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("error parse [" + obj + "] to json", e);
        }
    }

    /**
     * 实体对象转格式化输出的json字符串(用于日志打印)
     *
     * @param obj 实体对象
     * @return 格式化输出的json字符串
     */
    public static String toFormatJson(Object obj) {
        try {
            Map<String, String> collect =
                    ((Properties) obj)
                            .entrySet().stream()
                                    .collect(
                                            toMap(
                                                    v -> v.getKey().toString(),
                                                    v -> v.getValue().toString()));
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(collect);
        } catch (Exception e) {
            throw new RuntimeException("error parse [" + obj + "] to json", e);
        }
    }

    /**
     * 实体对象转byte数组
     *
     * @param obj 实体对象
     * @return byte数组
     */
    public static byte[] toBytes(Object obj) {
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("error parse [" + obj + "] to json", e);
        }
    }


    /**
     *
     * @param
     * @return
     * @throws Exception
     */
    public static Map<String, Object> objectToMap(String content) throws Exception {
        return objectMapper.readValue(content, Map.class);
    }
}
