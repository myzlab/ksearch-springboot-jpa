package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KIrish extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("irish").toString();
    }
}
