package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KJsonb extends KDataType {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("JSONB").toString();
    }
}
