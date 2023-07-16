package com.myzlab.k.optional;

public class KOptionalBoolean {

    private final Boolean value;
    
    private KOptionalBoolean() {
        super();
        
        this.value = null;
    }
    
    private KOptionalBoolean(
        final Boolean value
    ) {
        super();
        
        this.value = value;
    }
    
    public static KOptionalBoolean getInstance(
        final Boolean value
    ) {
        return new KOptionalBoolean(value);
    }
    
    public Boolean get() {
        return value;
    }
    
    public boolean isPresent() {
        return value != null;
    }
}
