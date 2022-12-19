package com.myzlab.k.optional;

import com.myzlab.k.KValLocalDateTimeField;

public class KOptionalKValLocalDateTimeField {

    private final KValLocalDateTimeField kValLocalDateTimeField;
    
    private KOptionalKValLocalDateTimeField() {
        super();
        
        this.kValLocalDateTimeField = null;
    }
    
    private KOptionalKValLocalDateTimeField(
        final KValLocalDateTimeField kValLocalDateTimeField
    ) {
        super();
        
        this.kValLocalDateTimeField = kValLocalDateTimeField;
    }
    
    public static KOptionalKValLocalDateTimeField getInstance(
        final KValLocalDateTimeField kValLocalDateTimeField
    ) {
        return new KOptionalKValLocalDateTimeField(kValLocalDateTimeField);
    }
    
    public KValLocalDateTimeField get() {
        return kValLocalDateTimeField;
    }
    
    public boolean isPresent() {
        return kValLocalDateTimeField != null;
    }
}
