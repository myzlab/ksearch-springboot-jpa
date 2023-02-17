package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KSqlAscii extends KEncoding {
    
    @Override
    public String toSql() {
        return "SQL_ASCII";
    }
}
