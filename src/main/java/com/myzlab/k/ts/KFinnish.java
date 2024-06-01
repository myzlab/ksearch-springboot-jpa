package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KFinnish extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("finnish").toString();
    }
}
