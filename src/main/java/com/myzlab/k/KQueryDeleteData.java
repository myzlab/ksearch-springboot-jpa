package com.myzlab.k;

import java.util.ArrayList;
import java.util.List;

public class KQueryDeleteData extends KQueryGenericData {
    
    protected int usingTablesAdded;
    
    public KQueryDeleteData() {
        super();
        
        this.usingTablesAdded = 0;
    }

    public KQueryDeleteData(
        final StringBuilder sb,
        final List<Object> params,
        final List<KEdge> kEdges,
        final List<KNode> kNodes,
        final int usingTablesAdded,
        final List<KBaseColumn> kBaseColumns
    ) {
        super(kBaseColumns, sb, params, kEdges, kNodes);
        
        this.usingTablesAdded = usingTablesAdded;
    }
    
    protected KQueryDeleteData cloneMe() {
        return new KQueryDeleteData(
            new StringBuilder(this.sb),
            new ArrayList<>(this.params),
            new ArrayList<>(this.kEdges),
            new ArrayList<>(this.kNodes),
            this.usingTablesAdded,
            this.kBaseColumns
        );
    }
}
