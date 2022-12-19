package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KTimeWithoutTimeZone extends KDataType {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("TIME WITHOUT TIME ZONE").toString();
    }
}
