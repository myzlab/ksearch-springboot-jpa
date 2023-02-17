package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KGb18030 extends KEncoding {
    
    @Override
    public String toSql() {
        return "GB18030";
    }
}
