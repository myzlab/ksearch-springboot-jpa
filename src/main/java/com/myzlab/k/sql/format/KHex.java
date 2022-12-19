package com.myzlab.k.sql.format;

import com.myzlab.k.KFormat;

public class KHex extends KFormat {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("HEX").toString();
    }
}
