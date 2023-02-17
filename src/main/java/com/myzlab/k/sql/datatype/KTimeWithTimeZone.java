package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KTimeWithTimeZone extends KDataType {
    
    @Override
    public String toSql() {
        return "TIME WITH TIME ZONE";
    }
}
