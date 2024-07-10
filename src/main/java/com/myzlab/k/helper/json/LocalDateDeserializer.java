package com.myzlab.k.helper.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.myzlab.k.helper.KExceptionHelper;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class LocalDateDeserializer extends JsonDeserializer<LocalDate> {
    
    private static final List<DateTimeFormatter> FORMATTERS = Arrays.asList(
        DateTimeFormatter.ofPattern("MM/dd/yyyy"),
        DateTimeFormatter.ofPattern("yyyy/MM/dd"),
        DateTimeFormatter.ofPattern("yyyyMMdd")
    );
    
    @Override
    public LocalDate deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        final String src = p.getText();

        try {
            return LocalDate.parse(p.getText(), DateTimeFormatter.ISO_LOCAL_DATE);
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
