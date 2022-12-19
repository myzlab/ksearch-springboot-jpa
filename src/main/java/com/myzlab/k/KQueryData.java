package com.myzlab.k;

import java.util.ArrayList;
import java.util.List;

public class KQueryData extends KQueryGenericData {

    protected int columnsAdded;
    protected int tablesAdded;
    protected boolean distinctOn;
    
    public KQueryData() {
        super();
        
        this.columnsAdded = 0;
        this.tablesAdded = 0;
        this.distinctOn = false;
    }

    public KQueryData(
        final List<KBaseColumn> kBaseColumns, 
        final StringBuilder sb,
        final int columnsAdded,
        final int tablesAdded,
        final List<Object> params,
        final List<KEdge> kEdges,
        final List<KNode> kNodes,
        final boolean distinctOn
    ) {
        super(kBaseColumns, sb, params, kEdges, kNodes);
        
        this.columnsAdded = columnsAdded;
        this.tablesAdded = tablesAdded;
        this.distinctOn = distinctOn;
    }
    
    protected KQueryData cloneMe() {
        return new KQueryData(
            new ArrayList<>(this.kBaseColumns),
            new StringBuilder(this.sb),
            this.columnsAdded,
            this.tablesAdded,
            new ArrayList<>(this.params),
            new ArrayList<>(this.kEdges),
            new ArrayList<>(this.kNodes),
            this.distinctOn
        );
    }
}
