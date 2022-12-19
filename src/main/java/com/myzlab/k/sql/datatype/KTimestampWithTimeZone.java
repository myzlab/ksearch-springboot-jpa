package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KTimestampWithTimeZone extends KDataType {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("TIMESTAMP WITH TIME ZONE").toString();
    }
}
