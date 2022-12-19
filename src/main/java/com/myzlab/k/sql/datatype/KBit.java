package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KBit extends KDataType {

    private final Integer length;
    
    public KBit() {
        this.length = null;
    }
    
    public KBit(
        final Integer length
    ) {
        this.length = length;
    }
    
    @Override
    public String toSql() {
        final StringBuilder sb = new StringBuilder().append("BIT");
        
        if (length != null) {
            sb.append("(").append(length).append(")");
        }
        
        return sb.toString();
    }
}
