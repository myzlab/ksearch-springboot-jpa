package com.myzlab.k.optional;

import com.myzlab.k.KValTextField;

public class KOptionalKValTextField {

    private final KValTextField kValTextField;
    
    private KOptionalKValTextField() {
        super();
        
        this.kValTextField = null;
    }
    
    private KOptionalKValTextField(
        final KValTextField kValTextField
    ) {
        super();
        
        this.kValTextField = kValTextField;
    }
    
    public static KOptionalKValTextField getInstance(
        final KValTextField kValTextField
    ) {
        return new KOptionalKValTextField(kValTextField);
    }
    
    public KValTextField get() {
        return kValTextField;
    }
    
    public boolean isPresent() {
        return kValTextField != null;
    }
}
