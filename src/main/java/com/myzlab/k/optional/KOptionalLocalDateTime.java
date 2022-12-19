package com.myzlab.k.optional;

import java.time.LocalDateTime;

public class KOptionalLocalDateTime {

    private final LocalDateTime localDateTime;
    
    private KOptionalLocalDateTime() {
        super();
        
        this.localDateTime = null;
    }
    
    private KOptionalLocalDateTime(
        final LocalDateTime localDateTime
    ) {
        super();
        
        this.localDateTime = localDateTime;
    }
    
    public static KOptionalLocalDateTime getInstance(
        final LocalDateTime localDateTime
    ) {
        return new KOptionalLocalDateTime(localDateTime);
    }
    
    public LocalDateTime get() {
        return localDateTime;
    }
    
    public boolean isPresent() {
        return localDateTime != null;
    }
}
