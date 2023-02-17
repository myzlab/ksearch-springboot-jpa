package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KShiftJis2004 extends KEncoding {
    
    @Override
    public String toSql() {
        return "SHIFT_JIS_2004";
    }
}
