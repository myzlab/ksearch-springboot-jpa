package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KTimeWithTimeZone extends KDataType {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("TIME WITH TIME ZONE").toString();
    }
}
