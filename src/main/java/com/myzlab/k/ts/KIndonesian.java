package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KIndonesian extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("indonesian").toString();
    }
}
