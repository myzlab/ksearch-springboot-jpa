package com.myzlab.k.sql.extract.field;

import com.myzlab.k.KExtractField;

public class KMinute extends KExtractField {
    
    @Override
    public String toSql() {
        return "MINUTE";
    }
}
