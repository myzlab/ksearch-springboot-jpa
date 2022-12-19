package com.myzlab.k;

public class KInsertIntoOnConflictTargeted extends KQueryInsertInto {

    private KInsertIntoOnConflictTargeted(
        final KExecutor kExecutor
    ) {
        super(kExecutor);
    }
 
    private KInsertIntoOnConflictTargeted(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KColumn kColumn
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        KUtils.assertNotNull(kColumn, "kColumn");
        
        this.process(kColumn);
    }
    
    private KInsertIntoOnConflictTargeted(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final String constraint
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        KUtils.assertNotNull(constraint, "constraint");
        
        this.process(constraint);
    }
    
    protected static KInsertIntoOnConflictTargeted getInstanceOnColumn(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KColumn kColumn
    ) {
        return new KInsertIntoOnConflictTargeted(kExecutor, kQueryInsertIntoData, kColumn);
    }
    
    protected static KInsertIntoOnConflictTargeted getInstanceOnConstraint(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final String constraint
    ) {
        return new KInsertIntoOnConflictTargeted(kExecutor, kQueryInsertIntoData, constraint);
    }
    
    public KInsertIntoOnConflictDoNothing doNothing() {
        return KInsertIntoOnConflictDoNothing.getInstance(this.k, this.kQueryInsertIntoData);
    }
    
    public KInsertIntoOnConflictDoUpdate doUpdate() {
        return KInsertIntoOnConflictDoUpdate.getInstance(this.k, this.kQueryInsertIntoData);
    }

    private void process(
        final KColumn kColumn
    ) {
        this.kQueryInsertIntoData.sb.append(" (").append(kColumn.name).append(")");
    }
    
    private void process(
        final String constraint
    ) {
        this.kQueryInsertIntoData.sb.append(" ON CONSTRAINT \"").append(constraint).append("\"");
    }
}
