package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KKoi8u extends KEncoding {
    
    @Override
    public String toSql() {
        return "KOI8U";
    }
}
