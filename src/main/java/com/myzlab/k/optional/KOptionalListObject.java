package com.myzlab.k.optional;

import java.util.List;

public class KOptionalListObject {

    private List<Object> values;
    private final boolean omitOnEmptyArray;
    
    private KOptionalListObject() {
        super();
        
        this.values = null;
        this.omitOnEmptyArray = false;
    }
    
    private KOptionalListObject(
        final List<Object> values,
        final boolean omitOnEmptyArray
    ) {
        super();
        
        this.values = values;
        this.omitOnEmptyArray = omitOnEmptyArray;
    }
    
    public static KOptionalListObject getInstance(
        final List<Object> values,
        final boolean omitOnEmptyArray
    ) {
        return new KOptionalListObject(values, omitOnEmptyArray);
    }
    
    public List<Object> get() {
        return values;
    }
    
    public boolean isPresent() {
        if (values == null) {
            return false;
        }
        
        if (!omitOnEmptyArray) {
            return true;
        }
        
        return !values.isEmpty();
    }
}
