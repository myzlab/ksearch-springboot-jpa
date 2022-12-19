package com.myzlab.k.optional;

public class KOptionalNumber {

    private final Number number;
    
    private KOptionalNumber() {
        super();
        
        this.number = null;
    }
    
    private KOptionalNumber(
        final Number number
    ) {
        super();
        
        this.number = number;
    }
    
    public static KOptionalNumber getInstance(
        final Number number
    ) {
        return new KOptionalNumber(number);
    }
    
    public Number get() {
        return number;
    }
    
    public boolean isPresent() {
        return number != null;
    }
}
