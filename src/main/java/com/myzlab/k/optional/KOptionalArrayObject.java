package com.myzlab.k.optional;

public class KOptionalArrayObject {

    private Object[] values;
    private final boolean omitOnEmptyArray;
    
    private KOptionalArrayObject() {
        super();
        
        this.values = null;
        this.omitOnEmptyArray = false;
    }
    
    private KOptionalArrayObject(
        final Object[] values,
        final boolean omitOnEmptyArray
    ) {
        super();
        
        this.values = values;
        this.omitOnEmptyArray = omitOnEmptyArray;
    }
    
    public static KOptionalArrayObject getInstance(
        final Object[] values,
        final boolean omitOnEmptyArray
    ) {
        return new KOptionalArrayObject(values, omitOnEmptyArray);
    }
    
    public Object[] get() {
        return values;
    }
    
    public boolean isPresent() {
        if (values == null) {
            return false;
        }
        
        if (!omitOnEmptyArray) {
            return true;
        }
        
        return values.length > 0;
    }
}
