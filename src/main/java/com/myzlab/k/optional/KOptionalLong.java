package com.myzlab.k.optional;

public class KOptionalLong {

    private final Long value;
    
    private KOptionalLong() {
        super();
        
        this.value = null;
    }
    
    private KOptionalLong(
        final Long value
    ) {
        super();
        
        this.value = value;
    }
    
    public static KOptionalLong getNullInstance() {
        return new KOptionalLong();
    }
    
    public static KOptionalLong getInstance(
        final Long value
    ) {
        return new KOptionalLong(value);
    }
    
    public Long get() {
        return value;
    }
    
    public boolean isPresent() {
        return value != null;
    }
}
