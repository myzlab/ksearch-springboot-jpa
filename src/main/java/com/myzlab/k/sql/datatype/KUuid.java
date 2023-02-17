package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KUuid extends KDataType {
    
    @Override
    public String toSql() {
        return "UUID";
    }
}
