package com.myzlab.k.optional;

import java.time.LocalDate;

public class KOptionalLocalDate {

    private final LocalDate localDate;
    
    private KOptionalLocalDate() {
        super();
        
        this.localDate = null;
    }
    
    private KOptionalLocalDate(
        final LocalDate localDate
    ) {
        super();
        
        this.localDate = localDate;
    }
    
    public static KOptionalLocalDate getInstance(
        final LocalDate localDate
    ) {
        return new KOptionalLocalDate(localDate);
    }
    
    public LocalDate get() {
        return localDate;
    }
    
    public boolean isPresent() {
        return localDate != null;
    }
}
