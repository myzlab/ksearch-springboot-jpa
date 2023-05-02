package com.myzlab.k.optional;

import com.myzlab.k.KCondition;

public class KOptionalKCondition {

    private final KCondition kCondition;
    
    private KOptionalKCondition() {
        super();
        
        this.kCondition = null;
    }
    
    private KOptionalKCondition(
        final KCondition kCondition
    ) {
        super();
        
        this.kCondition = kCondition;
    }
    
    public static KOptionalKCondition getInstance(
        final KCondition kCondition
    ) {
        return new KOptionalKCondition(kCondition);
    }
    
    public KCondition get() {
        return kCondition;
    }
    
    public boolean isPresent() {
        return kCondition != null;
    }
}
