package com.myzlab.k;

import java.util.ArrayList;

public abstract class KQueryUpdate extends KGenericQuery {
    
    protected KExecutor k;
    protected KQueryUpdateData kQueryUpdateData;

    protected KQueryUpdate() {
        this.kQueryUpdateData = new KQueryUpdateData();
    }
    
    protected KQueryUpdate(
        final KExecutor kExecutor
    ) {
        this.kQueryUpdateData = new KQueryUpdateData();
        this.k = kExecutor;
    }
    
    public KQueryUpdate(
        final KQueryUpdateData kQueryUpdateData,
        final KExecutor kExecutor
    ) {
        this.kQueryUpdateData = kQueryUpdateData;
        this.k = kExecutor;
    }
    
    protected int executeSingle() {
        return KQueryUtils.executeSingle(this.k, this.kQueryUpdateData);
    }
    
    protected KCollection<KRow> executeMapping() {
        return this.executeMapping(KRow.class);
    }
    
    protected <T extends KRow> KCollection<T> executeMapping(
        final Class<T> clazz
    ) {
        return KQueryUtils.multipleMapping(this.k, new ArrayList<>(), this.kQueryUpdateData, clazz);
    }
    
    @Override
    protected KQueryGenericData generateSubQueryData() {
        return this.kQueryUpdateData.cloneMe();
    }
}
