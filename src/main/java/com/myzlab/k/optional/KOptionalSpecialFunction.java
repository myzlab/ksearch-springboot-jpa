package com.myzlab.k.optional;

import com.myzlab.k.KSpecialFunction;

public class KOptionalSpecialFunction {

    private final KSpecialFunction kSpecialFunction;
    
    private KOptionalSpecialFunction() {
        super();
        
        this.kSpecialFunction = null;
    }
    
    private KOptionalSpecialFunction(
        final KSpecialFunction kSpecialFunction
    ) {
        super();
        
        this.kSpecialFunction = kSpecialFunction;
    }
    
    public static KOptionalSpecialFunction getInstance(
        final KSpecialFunction kSpecialFunction
    ) {
        return new KOptionalSpecialFunction(kSpecialFunction);
    }
    
    public KSpecialFunction get() {
        return kSpecialFunction;
    }
    
    public boolean isPresent() {
        return kSpecialFunction != null;
    }
}
