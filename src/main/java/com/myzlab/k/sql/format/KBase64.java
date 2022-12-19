package com.myzlab.k.sql.format;

import com.myzlab.k.KFormat;

public class KBase64 extends KFormat {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("BASE64").toString();
    }
}
