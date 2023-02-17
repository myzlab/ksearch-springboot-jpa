package com.myzlab.k.sql.extract.field;

import com.myzlab.k.KExtractField;

public class KEpoch extends KExtractField {
    
    @Override
    public String toSql() {
        return "EPOCH";
    }
}
