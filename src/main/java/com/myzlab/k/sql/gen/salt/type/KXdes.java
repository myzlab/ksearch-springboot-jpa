package com.myzlab.k.sql.gen.salt.type;

import com.myzlab.k.KGenSaltType;

public class KXdes extends KGenSaltType {
    
    @Override
    public String toSql() {
        return "xdes";
    }
}
