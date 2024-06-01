package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KRomanian extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("romanian").toString();
    }
}
