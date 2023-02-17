package com.myzlab.k.sql.extract.field;

import com.myzlab.k.KExtractField;

public class KHour extends KExtractField {
    
    @Override
    public String toSql() {
        return "HOUR";
    }
}
