package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KSimple extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("simple").toString();
    }
}
