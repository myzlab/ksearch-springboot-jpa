package com.myzlab.k.sql.extract.field;

import com.myzlab.k.KExtractField;

public class KMillennium extends KExtractField {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("MILLENNIUM").toString();
    }
}
