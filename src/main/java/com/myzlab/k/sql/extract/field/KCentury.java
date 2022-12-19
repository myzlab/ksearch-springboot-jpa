package com.myzlab.k.sql.extract.field;

import com.myzlab.k.KExtractField;

public class KCentury extends KExtractField {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("CENTURY").toString();
    }
}
