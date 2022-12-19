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
        final KColumn kColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        return KSetOnConflictDoUpdate.getInstance(this.k, this.kQueryInsertIntoData, kColumn, kColumnAllowedToSetUpdate);
    }
    
    public KSetOnConflictDoUpdate set(
        final KColumn kColumn,
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        return KSetOnConflictDoUpdate.getInstance(this.k, this.kQueryInsertIntoData, kColumn, new KColumn(new StringBuilder(((KRaw) kRaw).content), false));
    }
    
    public KSetOnConflictDoUpdate set(
        final KColumn kColumn,
        final KQuery kQuery
    ) {
        return KSetOnConflictDoUpdate.getInstance(this.k, this.kQueryInsertIntoData, kColumn, kQuery);
    }

    private void process() {
        this.kQueryInsertIntoData.sb.append(" DO UPDATE");
    }
}

