package com.myzlab.k.sql.encoding;

import com.myzlab.k.KEncoding;

public class KMuleInternal extends KEncoding {
    
    @Override
    public String toSql() {
        return "MULE_INTERNAL";
    }
}
