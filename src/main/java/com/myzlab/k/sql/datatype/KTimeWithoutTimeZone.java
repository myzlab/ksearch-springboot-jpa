package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KTimeWithoutTimeZone extends KDataType {
    
    @Override
    public String toSql() {
        return "TIME WITHOUT TIME ZONE";
    }
}
