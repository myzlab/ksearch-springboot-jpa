package com.myzlab.k.sql.extract.field;

import com.myzlab.k.KExtractField;

public class KMilliseconds extends KExtractField {
    
    @Override
    public String toSql() {
        return "MILLISECONDS";
    }
}
