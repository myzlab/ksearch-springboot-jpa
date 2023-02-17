package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KKoi8r extends KEncoding {
    
    @Override
    public String toSql() {
        return "KOI8R";
    }
}
