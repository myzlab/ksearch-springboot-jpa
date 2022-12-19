package com.myzlab.k.sql.extract.field;

import com.myzlab.k.KExtractField;

public class KDay extends KExtractField {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("DAY").toString();
    }
}
