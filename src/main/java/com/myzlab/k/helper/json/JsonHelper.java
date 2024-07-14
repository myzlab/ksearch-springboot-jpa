package com.myzlab.k.helper.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class JsonHelper {
    
    private static ObjectMapper MAPPER_INSTANCE = null;
    private static Gson GSON_INSTANCE = null;

    private static ObjectMapper getObjectMapper() {
        if (MAPPER_INSTANCE != null) {
            return MAPPER_INSTANCE;
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

        MAPPER_INSTANCE = objectMapper;
        
        return MAPPER_INSTANCE;
    }

    private static Gson getGson() {
        if (GSON_INSTANCE != null) {
            return GSON_INSTANCE;
        }
        
        final GsonBuilder gsonBuilder = new GsonBuilder();
        
        gsonBuilder.registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter());
        gsonBuilder.registerTypeAdapter(LocalDate.class, new LocalDateAdapter());
        gsonBuilder.registerTypeAdapter(LocalTime.class, new LocalTimeAdapter());
        
        GSON_INSTANCE = gsonBuilder.create();
        
        return GSON_INSTANCE;
    }
    
    public static String toJson(final Object src) {
        try {
            return getObjectMapper().writeValueAsString(src);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert object to JSON", e);
        }
    }
    
    public static <T> T fromJson(final String json, final Class<T> clazz) {
        try {
            return getGson().fromJson(json, clazz);
        } catch (JsonSyntaxException e) {
            throw new RuntimeException("Failed to convert JSON to object", e);
        }
    }
    
    public static <T> T fromJson(final String json, final TypeReference<T> typeRef) {
        try {
            return getObjectMapper().readValue(json, typeRef);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert JSON to object", e);
        }
    }
    
    public static <T extends Object> T fromJson(final String json, final Type typeOfT) {
        try {
            return getGson().fromJson(json, typeOfT);
        } catch (JsonSyntaxException e) {
            throw new RuntimeException("Failed to convert JSON to object", e);
        }
    }
}