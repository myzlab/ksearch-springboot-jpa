package com.myzlab.k.optional;

import com.myzlab.k.KValNumberField;

public class KOptionalKValNumberField {

    private final KValNumberField kValNumberField;
    
    private KOptionalKValNumberField() {
        super();
        
        this.kValNumberField = null;
    }
    
    private KOptionalKValNumberField(
        final KValNumberField kValNumberField
    ) {
        super();
        
        this.kValNumberField = kValNumberField;
    }
    
    public static KOptionalKValNumberField getInstance(
        final KValNumberField kValNumberField
    ) {
        return new KOptionalKValNumberField(kValNumberField);
    }
    
    public KValNumberField get() {
        return kValNumberField;
    }
    
    public boolean isPresent() {
        return kValNumberField != null;
    }
}
