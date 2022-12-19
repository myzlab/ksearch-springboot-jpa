package com.myzlab.k.optional;

public class KOptionalArrayObject {

    private Object[] values;
    private final boolean applyOnEmpty;
    
    private KOptionalArrayObject() {
        super();
        
        this.values = null;
        this.applyOnEmpty = false;
    }
    
    private KOptionalArrayObject(
        final Object[] values,
        final boolean applyOnEmpty
    ) {
        super();
        
        this.values = values;
        this.applyOnEmpty = applyOnEmpty;
    }
    
    public static KOptionalArrayObject getInstance(
        final Object[] values,
        final boolean applyOnEmpty
    ) {
        return new KOptionalArrayObject(values, applyOnEmpty);
    }
    
    public Object[] get() {
        return values;
    }
    
    public boolean isPresent() {
        if (values == null) {
            return false;
        }
        
        if (!applyOnEmpty) {
            return true;
        }
        
        return values.length > 0;
    }
}
