package com.myzlab.k.sql.gen.salt.type;

import com.myzlab.k.KGenSaltType;

public class KMd5 extends KGenSaltType {
    
    @Override
    public String toSql() {
        return "md5";
    }
}
