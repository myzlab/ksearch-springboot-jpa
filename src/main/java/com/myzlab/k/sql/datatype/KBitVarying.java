package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KBitVarying extends KDataType {

    private final Integer length;
    
    public KBitVarying() {
        this.length = null;
    }
    
    public KBitVarying(
        final Integer length
    ) {
        this.length = length;
    }
    
    @Override
    public String toSql() {
        final StringBuilder sb = new StringBuilder().append("VARBIT");
        
        if (length != null) {
            sb.append("(").append(length).append(")");
        }
        
        return sb.toString();
    }
}
