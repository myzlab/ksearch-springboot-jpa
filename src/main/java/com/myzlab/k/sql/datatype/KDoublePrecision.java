package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KDoublePrecision extends KDataType {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("DOUBLE PRECISION").toString();
    }
}
