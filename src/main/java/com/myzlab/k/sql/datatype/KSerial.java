package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KSerial extends KDataType {
    
    @Override
    public String toSql() {
        return "SERIAL";
    }
}
