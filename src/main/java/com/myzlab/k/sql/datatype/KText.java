package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KText extends KDataType {
    
    @Override
    public String toSql() {
        return "TEXT";
    }
}
