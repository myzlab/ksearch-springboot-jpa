package com.myzlab.k.optional;

import com.myzlab.k.KJoinDefinition;

public class KOptionalKJoinDefinition {

    private final KJoinDefinition kJoinDefinition;
    
    private KOptionalKJoinDefinition() {
        super();
        
        this.kJoinDefinition = null;
    }
    
    private KOptionalKJoinDefinition(
        final KJoinDefinition kJoinDefinition
    ) {
        super();
        
        this.kJoinDefinition = kJoinDefinition;
    }
    
    public static KOptionalKJoinDefinition getInstance(
        final KJoinDefinition kJoinDefinition
    ) {
        return new KOptionalKJoinDefinition(kJoinDefinition);
    }
    
    public KJoinDefinition get() {
        return kJoinDefinition;
    }
    
    public boolean isPresent() {
        return kJoinDefinition != null;
    }
}
