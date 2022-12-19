package com.myzlab.k.sql.extract.field;

import com.myzlab.k.KExtractField;

public class KDecade extends KExtractField {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("DECADE").toString();
    }
}
