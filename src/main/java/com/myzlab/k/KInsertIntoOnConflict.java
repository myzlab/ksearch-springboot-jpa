package com.myzlab.k;

public class KInsertIntoOnConflict extends KQueryInsertInto {

    private KInsertIntoOnConflict(
        final KExecutor kExecutor
    ) {
        super(kExecutor);
    }
 
    private KInsertIntoOnConflict(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process();
    }
    
    protected static KInsertIntoOnConflict getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData
    ) {
        return new KInsertIntoOnConflict(kExecutor, kQueryInsertIntoData);
    }
    
    public KInsertIntoOnConflictDoNothing doNothing() {
        return KInsertIntoOnConflictDoNothing.getInstance(this.k, this.kQueryInsertIntoData);
    }
    
    public KInsertIntoOnConflictTargeted targetColumn(
        final KColumn kColumn
    ) {
        return KInsertIntoOnConflictTargeted.getInstanceOnColumn(this.k, this.kQueryInsertIntoData, kColumn);
    }
    
    public KInsertIntoOnConflictTargeted targetConstraint(
        final String constraint
    ) {
        return KInsertIntoOnConflictTargeted.getInstanceOnConstraint(this.k, this.kQueryInsertIntoData, constraint);
    }

    private void process() {
        this.kQueryInsertIntoData.sb.append(" ON CONFLICT");
    }
}

