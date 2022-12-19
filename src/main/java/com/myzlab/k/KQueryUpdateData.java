package com.myzlab.k;

import java.util.ArrayList;
import java.util.List;

public class KQueryUpdateData extends KQueryGenericData {
    
    protected int fromTablesAdded;
    protected int setValuesAdded;
    
    public KQueryUpdateData() {
        super();
        
        this.fromTablesAdded = 0;
        this.setValuesAdded = 0;
    }

    public KQueryUpdateData(
        final StringBuilder sb,
        final List<Object> params,
        final List<KEdge> kEdges,
        final List<KNode> kNodes,
        final int fromTablesAdded,
        final int setValuesAdded,
        final List<KBaseColumn> kBaseColumns
    ) {
        super(kBaseColumns, sb, params, kEdges, kNodes);
        
        this.fromTablesAdded = fromTablesAdded;
        this.setValuesAdded = setValuesAdded;
    }
    
    protected KQueryUpdateData cloneMe() {
        return new KQueryUpdateData(
            new StringBuilder(this.sb),
            new ArrayList<>(this.params),
            new ArrayList<>(this.kEdges),
            new ArrayList<>(this.kNodes),
            this.fromTablesAdded,
            this.setValuesAdded,
            this.kBaseColumns
        );
    }
}
