package com.myzlab.k;

import com.myzlab.k.sql.datatype.column.KColumnJson;
import com.myzlab.k.sql.datatype.column.KColumnJsonb;
import com.myzlab.k.sql.datatype.column.KColumnUuid;

public abstract class KColumnDataType {
    
    protected String datatype;
    protected String castTo;

    public KColumnDataType(String datatype, String castTo) {
        this.datatype = datatype;
        this.castTo = castTo;
    }
    
    public static KColumnJson json() {
        return new KColumnJson();
    }
    
    public static KColumnJsonb jsonb() {
        return new KColumnJsonb();
    }
    
    public static KColumnUuid uuid() {
        return new KColumnUuid();
    }
}
