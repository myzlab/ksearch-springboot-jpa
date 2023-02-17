package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KGbk extends KEncoding {
    
    @Override
    public String toSql() {
        return "GBK";
    }
}
