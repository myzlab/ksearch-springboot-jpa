package com.myzlab.k.sql.extract.field;

import com.myzlab.k.KExtractField;

public class KWeek extends KExtractField {
    
    @Override
    public String toSql() {
        return "WEEK";
    }
}
