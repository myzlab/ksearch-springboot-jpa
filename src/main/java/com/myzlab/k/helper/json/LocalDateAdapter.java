package com.myzlab.k.helper.json;

import com.google.gson.*;
import com.myzlab.k.helper.KExceptionHelper;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class LocalDateAdapter implements JsonSerializer<LocalDate>, JsonDeserializer<LocalDate> {
    
    private static final List <DateTimeFormatter> FORMATTERS = Arrays.asList(
        DateTimeFormatter.ofPattern("MM/dd/yyyy"),
        DateTimeFormatter.ofPattern("yyyy/MM/dd"),
        DateTimeFormatter.ofPattern("yyyyMMdd")
    );

    @Override
    public JsonElement serialize(LocalDate src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(src.format(DateTimeFormatter.ISO_LOCAL_DATE));
    }

    @Override
    public LocalDate deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        final String src = json.getAsString();
        
        try {
            return LocalDate.parse(src, DateTimeFormatter.ISO_LOCAL_DATE);
        } catch (Exception e) {
            for (DateTimeFormatter formatter : FORMATTERS) {
                try {
                    return LocalDate.parse(src, formatter);
                } catch (Exception ignored) {}
            }

            throw KExceptionHelper.internalServerError("Text '" + src + "' could not be parsed (LocalDate)");
        }
    }
}