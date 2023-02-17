package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KWin1255 extends KEncoding {
    
    @Override
    public String toSql() {
        return "WIN1255";
    }
}
