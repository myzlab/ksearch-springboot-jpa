package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KBoolean extends KDataType {
    
    @Override
    public String toSql() {
        return "BOOLEAN";
    }
}
