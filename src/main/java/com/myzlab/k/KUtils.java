package com.myzlab.k;

import com.myzlab.k.helper.KExceptionHelper;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

public class KUtils {

    protected static String reverseParams(
        final KBaseColumn kBaseColumn
    ) {
        final String value = kBaseColumn.sb.toString();
        final List<Object> params = kBaseColumn.params;
                
        String newValue = value;
        
        for (final Object o : params) {
            if (o == null) {
                throw KExceptionHelper.internalServerError("'o' is required");
            }

            final String quote = o instanceof String ? "'" : "";

            newValue = newValue.replaceFirst(Pattern.quote("?"), quote + o.toString() + quote);
        }

        return newValue;
    }
    
    protected static void assertNotNull(
        final Object o,
        final String name
    ) {
        if (o == null) {
            throw KExceptionHelper.internalServerError("The '" + name + "' param is required"); 
        }
    }
    
    protected static void assertNotNullNotEmpty(
        final Object o,
        final String name
    ) {
        assertNotNullNotEmpty(o, name, true);
    }
    
    protected static void assertNotNullNotEmpty(
        final Object o,
        final String name,
        final boolean canContainNullvalues
    ) {
        if (o == null) {
            throw KExceptionHelper.internalServerError("The '" + name + "' param is required"); 
        }
        
        if (o instanceof Object[]) {
            if (((Object[]) o).length == 0) {
                throw KExceptionHelper.internalServerError("The '" + name + "' param cannot be empty");
            }
            
            for (final Object o_ : (Object[]) o) {
                if (o_ == null) {
                    throw KExceptionHelper.internalServerError("The '" + name + "' param cannot contain null values"); 
                }
            }
        }
        
        if (o instanceof Collection) {
            if (((Collection) o).isEmpty()) {
                throw KExceptionHelper.internalServerError("The '" + name + "' param cannot be empty");
            }
            
            for (final Object o_ : (Collection) o) {
                if (canContainNullvalues) {
                    continue;
                }
                
                if (o_ == null) {
                    throw KExceptionHelper.internalServerError("The '" + name + "' param cannot contain null values"); 
                }
            }
        }
    }
    
}
