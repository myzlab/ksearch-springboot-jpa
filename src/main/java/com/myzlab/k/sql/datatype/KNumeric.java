package com.myzlab.k.sql.datatype;

import com.myzlab.k.KDataType;

public class KNumeric extends KDataType {

    private final Integer precision;
    private final Integer scale;
    
    public KNumeric() {
        this.precision = null;
        this.scale = null;
    }
    
    public KNumeric(
        final Integer precision
    ) {
        this.precision = precision;
        this.scale = null;
    }
    
    public KNumeric(
        final Integer precision,
        final Integer scale
    ) {
        this.precision = precision;
        this.scale = scale;
    }
    
    @Override
    public String toSql() {
        final StringBuilder sb = new StringBuilder().append("NUMERIC");
        
        if (precision != null) {
            sb.append("(").append(precision);

            if (scale != null) {
                sb.append(", ").append(scale);
            }

            sb.append(")");
        }
        
        return sb.toString();
    }
}
