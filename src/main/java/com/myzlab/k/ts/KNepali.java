package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KNepali extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("nepali").toString();
    }
}
