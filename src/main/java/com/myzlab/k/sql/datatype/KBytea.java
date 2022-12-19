package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KBytea extends KDataType {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("BYTEA").toString();
    }
}
