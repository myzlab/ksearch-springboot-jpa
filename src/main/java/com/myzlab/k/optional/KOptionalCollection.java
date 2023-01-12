package com.myzlab.k.optional;

import java.util.Collection;

public class KOptionalCollection {

    private final Collection values;
    private final boolean omitOnEmptyCollection;
    
    private KOptionalCollection() {
        super();
        
        this.values = null;
        this.omitOnEmptyCollection = false;
    }
    
    private KOptionalCollection(
        final Collection values,
        final boolean omitOnEmptyCollection
    ) {
        super();
        
        this.values = values;
        this.omitOnEmptyCollection = omitOnEmptyCollection;
    }
    
    public static KOptionalCollection getInstance(
        final Collection values,
        final boolean omitOnEmptyCollection
    ) {
        return new KOptionalCollection(values, omitOnEmptyCollection);
    }
    
    public Collection get() {
        return values;
    }
    
    public boolean isPresent() {
        if (values == null) {
            return false;
        }
        
        if (!omitOnEmptyCollection) {
            return true;
        }
        
        return !values.isEmpty();
    }
}
