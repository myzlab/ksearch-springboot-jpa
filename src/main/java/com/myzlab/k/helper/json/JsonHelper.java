package com.myzlab.k.helper.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class JsonHelper {
    
    private static Gson INSTANCE = null;

    public static Gson getGson() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        
        final GsonBuilder gsonBuilder = new GsonBuilder();
        
        gsonBuilder.registerTypeAdapter(String.class, new StringAdapter());
        gsonBuilder.registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter());
        gsonBuilder.registerTypeAdapter(LocalDate.class, new LocalDateAdapter());
        gsonBuilder.registerTypeAdapter(LocalTime.class, new LocalTimeAdapter());
        gsonBuilder.disableHtmlEscaping();
        
        INSTANCE = gsonBuilder.create();
        
        return INSTANCE;
    }
    
    public static String toJson(Object src) {
        return getGson().toJson(src);
    }
    
    public static <T extends Object> T fromJson(String json, Type typeOfT) {
        return getGson().fromJson(json, typeOfT);
    }
}
