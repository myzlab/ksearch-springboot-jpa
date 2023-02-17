package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KEucJis2004 extends KEncoding {
    
    @Override
    public String toSql() {
        return "EUC_JIS_2004";
    }
}
