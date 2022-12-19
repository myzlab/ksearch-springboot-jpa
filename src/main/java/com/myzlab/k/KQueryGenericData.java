package com.myzlab.k;

import java.util.ArrayList;
import java.util.List;

public class KQueryGenericData {

    protected final List<KBaseColumn> kBaseColumns;
    protected final StringBuilder sb;
    protected final List<Object> params;
    protected final List<KEdge> kEdges;
    protected final List<KNode> kNodes;
    
    public KQueryGenericData() {
        super();
        
        this.kBaseColumns = new ArrayList<>();
        this.sb = new StringBuilder();
        this.params = new ArrayList<>();
        this.kEdges = new ArrayList<>();
        this.kNodes = new ArrayList<>();
    }

    public KQueryGenericData(
        final List<KBaseColumn> kBaseColumns, 
        final StringBuilder sb,
        final List<Object> params,
        final List<KEdge> kEdges,
        final List<KNode> kNodes
    ) {
        super();
        
        this.kBaseColumns = kBaseColumns;
        this.sb = sb;
        this.params = params;
        this.kEdges = kEdges;
        this.kNodes = kNodes;
    }
    
}
