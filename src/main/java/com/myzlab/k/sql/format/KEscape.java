package com.myzlab.k.sql.format;

import com.myzlab.k.KFormat;

public class KEscape extends KFormat {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("ESCAPE").toString();
    }
}
