package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KReal extends KDataType {
    
    @Override
    public String toSql() {
        return "REAL";
    }
}
