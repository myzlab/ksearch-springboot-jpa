package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KTimestampWithTimeZone extends KDataType {
    
    @Override
    public String toSql() {
        return "TIMESTAMP WITH TIME ZONE";
    }
}
