package com.myzlab.k.optional;

import java.util.Collection;

public class KOptionalCollection {

    private final Collection values;
    private final boolean applyOnEmpty;
    
    private KOptionalCollection() {
        super();
        
        this.values = null;
        this.applyOnEmpty = false;
    }
    
    private KOptionalCollection(
        final Collection values,
        final boolean applyOnEmpty
    ) {
        super();
        
        this.values = values;
        this.applyOnEmpty = applyOnEmpty;
    }
    
    public static KOptionalCollection getInstance(
        final Collection values,
        final boolean applyOnEmpty
    ) {
        return new KOptionalCollection(values, applyOnEmpty);
    }
    
    public Collection get() {
        return values;
    }
    
    public boolean isPresent() {
        if (values == null) {
            return false;
        }
        
        if (!applyOnEmpty) {
            return true;
        }
        
        return !values.isEmpty();
    }
}
