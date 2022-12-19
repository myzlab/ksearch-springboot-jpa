package com.myzlab.k;

import java.util.Objects;
import lombok.Data;

@Data
public class KNode {

    protected final Class<? extends KRow> source;
    protected final String aliasSource;

    private KNode(
        final Class<? extends KRow> source,
        final String aliasSource
    ) {
        this.source = source;
        this.aliasSource = aliasSource;
    }
    
    public static KNode getInstance(
        final Class<? extends KRow> source,
        final String aliasSource
    ) {
        return new KNode(source, aliasSource);
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KNode)) {
            return false;
        }
        
        return ((KNode) o).aliasSource.equals(this.aliasSource);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + Objects.hashCode(this.aliasSource);
        return hash;
    }
}
