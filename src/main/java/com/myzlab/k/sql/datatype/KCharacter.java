package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KCharacter extends KDataType {

    private final Integer length;
    
    public KCharacter() {
        this.length = null;
    }
    
    public KCharacter(
        final Integer length
    ) {
        this.length = length;
    }
    
    @Override
    public String toSql() {
        final StringBuilder sb = new StringBuilder().append("CHARACTER");
        
        if (length != null) {
            sb.append("(").append(length).append(")");
        }
        
        return sb.toString();
    }
}
