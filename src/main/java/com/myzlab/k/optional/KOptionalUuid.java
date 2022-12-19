package com.myzlab.k.optional;

import java.util.UUID;

public class KOptionalUuid {

    private final UUID uuid;
    
    private KOptionalUuid() {
        super();
        
        this.uuid = null;
    }
    
    private KOptionalUuid(
        final UUID uuid
    ) {
        super();
        
        this.uuid = uuid;
    }
    
    public static KOptionalUuid getInstance(
        final UUID uuid
    ) {
        return new KOptionalUuid(uuid);
    }
    
    public UUID get() {
        return uuid;
    }
    
    public boolean isPresent() {
        return uuid != null;
    }
}
