package com.myzlab.k;

import java.util.ArrayList;
import java.util.List;

public class KQueryInsertIntoData extends KQueryGenericData {
    
    protected int setValuesAdded;
    
    public KQueryInsertIntoData() {
        super();
        
        this.setValuesAdded = 0;
    }

    public KQueryInsertIntoData(
        final StringBuilder sb,
        final List<Object> params,
        final List<KEdge> kEdges,
        final List<KNode> kNodes,
        final int setValuesAdded,
        final List<KBaseColumn> kBaseColumns
    ) {
        super(kBaseColumns, sb, params, kEdges, kNodes);
        
        this.setValuesAdded = setValuesAdded;
    }
    
    protected KQueryInsertIntoData cloneMe() {
        return new KQueryInsertIntoData(
            new StringBuilder(this.sb),
            new ArrayList<>(this.params),
            new ArrayList<>(this.kEdges),
            new ArrayList<>(this.kNodes),
            this.setValuesAdded,
            this.kBaseColumns
        );
    }
}
