package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KArabic extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("arabic").toString();
    }
}
