package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KEucJp extends KEncoding {
    
    @Override
    public String toSql() {
        return "EUC_JP";
    }
}
