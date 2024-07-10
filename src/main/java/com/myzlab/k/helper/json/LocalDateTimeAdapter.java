package com.myzlab.k.helper.json;

import com.google.gson.*;
import com.myzlab.k.helper.KExceptionHelper;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class LocalDateTimeAdapter implements JsonSerializer<LocalDateTime>, JsonDeserializer<LocalDateTime> {

    private static final List<DateTimeFormatter> FORMATTERS = Arrays.asList(
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"),
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    );
    
    @Override
    public JsonElement serialize(LocalDateTime src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(src.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
    }

    @Override
    public LocalDateTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        final String src = json.getAsString();
        
        try {
            return ZonedDateTime.parse(src, DateTimeFormatter.ISO_ZONED_DATE_TIME).toLocalDateTime();
        } catch (Exception e) {
            for (DateTimeFormatter formatter : FORMATTERS) {
                try {
                    return LocalDateTime.parse(src, formatter);
                } catch (Exception ignored) {}
            }

            throw KExceptionHelper.internalServerError("Text '" + src + "' could not be parsed (LocalDateTime)");
        }
    }
}