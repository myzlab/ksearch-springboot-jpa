package com.myzlab.k.helper.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class JsonHelper {
    
    private static ObjectMapper INSTANCE = null;

    private static ObjectMapper getObjectMapper() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        
        final ObjectMapper objectMapper = new ObjectMapper();
        
        final SimpleModule module = new SimpleModule();
        
        module.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer());
        module.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer());
        module.addSerializer(LocalDate.class, new LocalDateSerializer());
        module.addDeserializer(LocalDate.class, new LocalDateDeserializer());
        module.addSerializer(LocalTime.class, new LocalTimeSerializer());
        module.addDeserializer(LocalTime.class, new LocalTimeDeserializer());
        
        // Registering JavaTimeModule for handling Java 8 date/time types
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.registerModule(module);
        
        // Setting to omit fields with null values
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        
        INSTANCE = objectMapper;
        
        return INSTANCE;
    }
    
    public static String toJson(Object src) {
        try {
            return getObjectMapper().writeValueAsString(src);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert object to JSON", e);
        }
    }
    
    public static <T> T fromJson(String json, Class<T> typeOfT) {
        try {
            return getObjectMapper().readValue(json, typeOfT);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert JSON to object", e);
        }
    }
    
    public static <T> T fromJson(String json, TypeReference<T> typeRef) {
        try {
            return getObjectMapper().readValue(json, typeRef);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert JSON to object", e);
        }
    }
}