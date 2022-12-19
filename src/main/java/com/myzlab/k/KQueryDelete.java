package com.myzlab.k;

import java.util.ArrayList;

public abstract class KQueryDelete {
    
    protected KExecutor k;
    protected KQueryDeleteData kQueryDeleteData;

    protected KQueryDelete() {
        this.kQueryDeleteData = new KQueryDeleteData();
    }
    
    protected KQueryDelete(
        final KExecutor kExecutor
    ) {
        this.kQueryDeleteData = new KQueryDeleteData();
        this.k = kExecutor;
    }
    
    public KQueryDelete(
        final KQueryDeleteData kQueryDeleteData,
        final KExecutor kExecutor
    ) {
        this.kQueryDeleteData = kQueryDeleteData;
        this.k = kExecutor;
    }
    
    protected int executeSingle() {
        return KQueryUtils.executeSingle(this.k, this.kQueryDeleteData);
    }
    
    protected KCollection<KRow> executeMapping() {
        return this.executeMapping(KRow.class);
    }
    
    protected <T extends KRow> KCollection<T> executeMapping(
        final Class<T> clazz
    ) {
        return KQueryUtils.multipleMapping(this.k, new ArrayList<>(), this.kQueryDeleteData, clazz);
    }
    
}
