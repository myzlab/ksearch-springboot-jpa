package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KCharacterVarying extends KDataType {

    private final Integer length;
    
    public KCharacterVarying() {
        this.length = null;
    }
    
    public KCharacterVarying(
        final Integer length
    ) {
        this.length = length;
    }
    
    @Override
    public String toSql() {
        final StringBuilder sb = new StringBuilder().append("VARCHAR");
        
        if (length != null) {
            sb.append("(").append(length).append(")");
        }
        
        return sb.toString();
    }
}
