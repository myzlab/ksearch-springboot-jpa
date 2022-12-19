package com.myzlab.k.optional;

public class KOptionalString {

    private final String value;
    
    private KOptionalString() {
        super();
        
        this.value = null;
    }
    
    private KOptionalString(
        final String value
    ) {
        super();
        
        this.value = value;
    }
    
    public static KOptionalString getInstance(
        final String value
    ) {
        return new KOptionalString(value);
    }
    
    public String get() {
        return value;
    }
    
    public boolean isPresent() {
        return value != null;
    }
}
