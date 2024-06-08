package com.myzlab.k;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public abstract class KBaseValField extends KBaseColumnCastable {
    
    protected KBaseValField() {
        super();
    }
    
    protected KBaseValField(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    protected KBaseValField(
        final String val
    ) {
        super(val);
    }
    
    protected KBaseValField(
        final String[] val
    ) {
        super(val);
    }
    
    protected KBaseValField(
        final Boolean val
    ) {
        super(val);
    }
    
    protected KBaseValField(
        final Number val
    ) {
        super(val);
    }
    
    protected KBaseValField(
        final Number[] val
    ) {
        super(val);
    }
    
    protected KBaseValField(
        final UUID val
    ) {
        super(val);
    }
    
    protected KBaseValField(
        final LocalDate val
    ) {
        super(val);
    }
    
    protected KBaseValField(
        final LocalDateTime val
    ) {
        super(val);
    }
}
