package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;

public class KInsertIntoOnConflictDoNothing extends KQueryInsertInto {

    private KInsertIntoOnConflictDoNothing(
        final KExecutor kExecutor
    ) {
        super(kExecutor);
    }
 
    private KInsertIntoOnConflictDoNothing(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process();
    }
    
    protected static KInsertIntoOnConflictDoNothing getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData
    ) {
        return new KInsertIntoOnConflictDoNothing(kExecutor, kQueryInsertIntoData);
    }
    
    public KReturningOnConflictDoUpdate returning(
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        return KReturningOnConflictDoUpdate.getInstance(this.k, this.kQueryInsertIntoData, kColumnsAllowedToReturning);
    }

    private void process() {
        this.kQueryInsertIntoData.sb.append(" DO NOTHING");
    }
    
    public int execute() {
        return super.executeSingle();
    }
}

