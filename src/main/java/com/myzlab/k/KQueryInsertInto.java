package com.myzlab.k;

import java.util.ArrayList;

public abstract class KQueryInsertInto {
    
    protected KExecutor k;
    protected KQueryInsertIntoData kQueryInsertIntoData;

    protected KQueryInsertInto() {
        this.kQueryInsertIntoData = new KQueryInsertIntoData();
    }
    
    protected KQueryInsertInto(
        final KExecutor kExecutor
    ) {
        this.kQueryInsertIntoData = new KQueryInsertIntoData();
        this.k = kExecutor;
    }
    
    public KQueryInsertInto(
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KExecutor kExecutor
    ) {
        this.kQueryInsertIntoData = kQueryInsertIntoData;
        this.k = kExecutor;
    }
    
    protected int executeSingle() {
        return KQueryUtils.executeSingle(this.k, this.kQueryInsertIntoData);
    }
    
    protected KCollection<KRow> executeMapping() {
        return this.executeMapping(KRow.class);
    }
    
    protected <T extends KRow> KCollection<T> executeMapping(
        final Class<T> clazz
    ) {
        return KQueryUtils.multipleMapping(this.k, new ArrayList<>(), this.kQueryInsertIntoData, clazz);
    }
    
}
