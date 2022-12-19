package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KTimestampWithoutTimeZone extends KDataType {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("TIMESTAMP WITHOUT TIME ZONE").toString();
    }
}
