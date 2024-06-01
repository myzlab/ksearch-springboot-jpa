package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KPortuguese extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("portuguese").toString();
    }
}
