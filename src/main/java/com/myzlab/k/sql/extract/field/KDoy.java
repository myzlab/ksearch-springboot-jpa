package com.myzlab.k.sql.extract.field;

import com.myzlab.k.KExtractField;

public class KDoy extends KExtractField {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("DOY").toString();
    }
}
