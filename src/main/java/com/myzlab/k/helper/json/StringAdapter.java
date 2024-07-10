package com.myzlab.k.helper.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class StringAdapter implements JsonSerializer<String> {
    @Override
    public JsonElement serialize(String src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(src);
    }
}