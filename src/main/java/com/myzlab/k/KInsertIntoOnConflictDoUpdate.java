package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;

public class KInsertIntoOnConflictDoUpdate extends KQueryInsertInto {

    private KInsertIntoOnConflictDoUpdate(
        final KExecutor kExecutor
    ) {
        super(kExecutor);
    }
 
    private KInsertIntoOnConflictDoUpdate(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process();
    }
    
    protected static KInsertIntoOnConflictDoUpdate getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData
    ) {
        return new KInsertIntoOnConflictDoUpdate(kExecutor, kQueryInsertIntoData);
    }
    
    public KSetOnConflictDoUpdate set(
        final KTableColumn kTableColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        return KSetOnConflictDoUpdate.getInstance(this.k, this.kQueryInsertIntoData, kTableColumn, kColumnAllowedToSetUpdate);
    }
    
    public KSetOnConflictDoUpdate set(
        final KTableColumn kTableColumn,
        final KQuery kQuery
    ) {
        return KSetOnConflictDoUpdate.getInstance(this.k, this.kQueryInsertIntoData, kTableColumn, kQuery);
    }

    private void process() {
        this.kQueryInsertIntoData.sb.append(" DO UPDATE");
    }
}

