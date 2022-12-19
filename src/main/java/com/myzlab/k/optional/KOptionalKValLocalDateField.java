package com.myzlab.k.optional;

import com.myzlab.k.KValLocalDateField;

public class KOptionalKValLocalDateField {

    private final KValLocalDateField kValLocalDateField;
    
    private KOptionalKValLocalDateField() {
        super();
        
        this.kValLocalDateField = null;
    }
    
    private KOptionalKValLocalDateField(
        final KValLocalDateField kValLocalDateField
    ) {
        super();
        
        this.kValLocalDateField = kValLocalDateField;
    }
    
    public static KOptionalKValLocalDateField getInstance(
        final KValLocalDateField kValLocalDateField
    ) {
        return new KOptionalKValLocalDateField(kValLocalDateField);
    }
    
    public KValLocalDateField get() {
        return kValLocalDateField;
    }
    
    public boolean isPresent() {
        return kValLocalDateField != null;
    }
}
