package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KSjis extends KEncoding {
    
    @Override
    public String toSql() {
        return "SJIS";
    }
}
