package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KBigint extends KDataType {
    
    @Override
    public String toSql() {
        return "BIGINT";
    }
}
