package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KIso88597 extends KEncoding {
    
    @Override
    public String toSql() {
        return "ISO_8859_7";
    }
}
