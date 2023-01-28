package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;

public class KSetOnConflictDoUpdate extends KQueryInsertInto {

    private KSetOnConflictDoUpdate() {
        super();
    }
    
    private KSetOnConflictDoUpdate(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KTableColumn kTableColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process(kTableColumn, kColumnAllowedToSetUpdate);
    }
    
    private KSetOnConflictDoUpdate(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KTableColumn kTableColumn,
        final KQuery kQuery
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process(kTableColumn, kQuery);
    }
    
    protected static KSetOnConflictDoUpdate getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KTableColumn kTableColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        return new KSetOnConflictDoUpdate(kExecutor, kQueryInsertIntoData, kTableColumn, kColumnAllowedToSetUpdate);
    }
    
    protected static KSetOnConflictDoUpdate getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KTableColumn kTableColumn,
        final KQuery kQuery
    ) {
        return new KSetOnConflictDoUpdate(kExecutor, kQueryInsertIntoData, kTableColumn, kQuery);
    }
    
    public KSetOnConflictDoUpdate set(
        final KTableColumn kTableColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        this.process(kTableColumn, kColumnAllowedToSetUpdate);
        
        return this;
    }
    
    public KSetOnConflictDoUpdate set(
        final KTableColumn kTableColumn,
        final KQuery kQuery
    ) {
        this.process(kTableColumn, kQuery);
        
        return this;
    }
    
    public KReturningInsertInto returning(
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        return KReturningInsertInto.getInstance(this.k, this.kQueryInsertIntoData, kColumnsAllowedToReturning);
    }
    
    private void process(
        final KTableColumn kTableColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        KUtils.assertNotNull(kTableColumn, "kTableColumn");
        KUtils.assertNotNull(kColumnAllowedToSetUpdate, "kColumnAllowedToSetUpdate");
        
        if (this.kQueryInsertIntoData.setValuesAdded == 0) {
            this.kQueryInsertIntoData.sb.append(" SET ");
        } else {
            this.kQueryInsertIntoData.sb.append(", ");
        }
        
        this.kQueryInsertIntoData.setValuesAdded++;
        
        this.kQueryInsertIntoData.params.addAll(kColumnAllowedToSetUpdate.getParams());
        
        String value = kColumnAllowedToSetUpdate.getSqlToSet();
        
        if (value.contains(value))
        
        this.kQueryInsertIntoData.sb.append(kTableColumn.name).append(" = ").append(kColumnAllowedToSetUpdate.getSqlToSet());
    }
    
    private void process(
        final KTableColumn kTableColumn,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kTableColumn, "kTableColumn");
        KUtils.assertNotNull(kQuery, "kQuery");
        
        if (this.kQueryInsertIntoData.setValuesAdded == 0) {
            this.kQueryInsertIntoData.sb.append(" SET ");
        } else {
            this.kQueryInsertIntoData.sb.append(", ");
        }
        
        this.kQueryInsertIntoData.setValuesAdded++;
        
        final KQueryGenericData subQuery = kQuery.generateSubQueryData();
        
        this.kQueryInsertIntoData.params.addAll(subQuery.params);
        this.kQueryInsertIntoData.sb.append(kTableColumn.name).append(" = (").append(subQuery.sb).append(")");
    }
    
    public int execute() {
        return super.executeSingle();
    }
}