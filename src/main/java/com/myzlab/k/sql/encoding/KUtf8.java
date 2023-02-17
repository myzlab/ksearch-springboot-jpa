package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KUtf8 extends KEncoding {
    
    @Override
    public String toSql() {
        return "UTF8";
    }
}
