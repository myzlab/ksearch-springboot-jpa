package com.myzlab.k;

import com.myzlab.k.helper.KExceptionHelper;

public class KInsertInto extends KQueryInsertInto {

    private KInsertInto(
        final KExecutor kExecutor
    ) {
        super(kExecutor);
    }
    
    private KInsertInto(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KTable kTable
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process(kTable);
    }
 
    private KInsertInto(
        final KExecutor kExecutor,
        final KTable kTable
    ) {
        super(kExecutor);
        
        this.process(kTable);
    }
    
    protected static KInsertInto getInstance(
        final KExecutor kExecutor,
        final KTable kTable
    ) {
        return new KInsertInto(kExecutor, kTable);
    }
    
    protected static KInsertInto getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KTable kTable
    ) {
        return new KInsertInto(kExecutor, kQueryInsertIntoData, kTable);
    }
    
    public KInsertIntoColumned columns(
        final KColumn... kColumns
    ) {
        return KInsertIntoColumned.getInstance(this.k, this.kQueryInsertIntoData, kColumns);
    }
    
    private void process(
        final KTable kTable
    ) {
        if (kTable == null) {
            throw KExceptionHelper.internalServerError("The 'kTable' param is required"); 
        }
        
        if (kTable.isRoot) {
            this.kQueryInsertIntoData.kNodes.add(KNode.getInstance(kTable.getKRowClass(), kTable.alias));
        }
        
        this.kQueryInsertIntoData.sb.append(kQueryInsertIntoData.sb.length() > 0 ? " " : "").append("INSERT INTO ").append(kTable.toSql(false));
        
        if (kTable.kQueryData != null) {
            this.kQueryInsertIntoData.params.addAll(kTable.kQueryData.params);
        }
    }
}
