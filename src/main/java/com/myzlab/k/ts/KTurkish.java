package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KTurkish extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("turkish").toString();
    }
}
