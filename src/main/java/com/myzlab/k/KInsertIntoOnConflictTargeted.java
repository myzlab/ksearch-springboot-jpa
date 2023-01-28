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
        final KTableColumn kTableColumn
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        KUtils.assertNotNull(kTableColumn, "kTableColumn");
        
        this.process(kTableColumn);
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
        final KTableColumn kTableColumn
    ) {
        return new KInsertIntoOnConflictTargeted(kExecutor, kQueryInsertIntoData, kTableColumn);
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
        final KTableColumn kTableColumn
    ) {
        this.kQueryInsertIntoData.sb.append(" (").append(kTableColumn.name).append(")");
    }
    
    private void process(
        final String constraint
    ) {
        this.kQueryInsertIntoData.sb.append(" ON CONSTRAINT \"").append(constraint).append("\"");
    }
}
