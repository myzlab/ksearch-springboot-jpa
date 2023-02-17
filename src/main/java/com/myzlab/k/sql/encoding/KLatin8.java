package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KLatin8 extends KEncoding {
    
    @Override
    public String toSql() {
        return "LATIN8";
    }
}
