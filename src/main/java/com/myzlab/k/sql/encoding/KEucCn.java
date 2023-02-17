package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KEucCn extends KEncoding {
    
    @Override
    public String toSql() {
        return "EUC_CN";
    }
}
