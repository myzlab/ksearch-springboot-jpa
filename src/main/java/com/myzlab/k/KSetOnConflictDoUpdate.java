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
        final KColumn kColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process(kColumn, kColumnAllowedToSetUpdate);
    }
    
    private KSetOnConflictDoUpdate(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KColumn kColumn,
        final KQuery kQuery
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process(kColumn, kQuery);
    }
    
    protected static KSetOnConflictDoUpdate getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KColumn kColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        return new KSetOnConflictDoUpdate(kExecutor, kQueryInsertIntoData, kColumn, kColumnAllowedToSetUpdate);
    }
    
    protected static KSetOnConflictDoUpdate getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KColumn kColumn,
        final KQuery kQuery
    ) {
        return new KSetOnConflictDoUpdate(kExecutor, kQueryInsertIntoData, kColumn, kQuery);
    }
    
    public KSetOnConflictDoUpdate set(
        final KColumn kColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        this.process(kColumn, kColumnAllowedToSetUpdate);
        
        return this;
    }
    
    public KSetOnConflictDoUpdate set(
        final KColumn kColumn,
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        this.process(kColumn, new KColumn(new StringBuilder(((KRaw) kRaw).content), false));
        
        return this;
    }
    
    public KSetOnConflictDoUpdate set(
        final KColumn kColumn,
        final KQuery kQuery
    ) {
        this.process(kColumn, kQuery);
        
        return this;
    }
    
    public KReturningOnConflictDoUpdate returning(
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        return KReturningOnConflictDoUpdate.getInstance(this.k, this.kQueryInsertIntoData, kColumnsAllowedToReturning);
    }
    
    private void process(
        final KColumn kColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        KUtils.assertNotNull(kColumn, "kColumn");
        KUtils.assertNotNull(kColumnAllowedToSetUpdate, "kColumnAllowedToSetUpdate");
        
        if (this.kQueryInsertIntoData.setValuesAdded == 0) {
            this.kQueryInsertIntoData.sb.append(" SET ");
        } else {
            this.kQueryInsertIntoData.sb.append(", ");
        }
        
        this.kQueryInsertIntoData.setValuesAdded++;
        
        this.kQueryInsertIntoData.params.addAll(kColumnAllowedToSetUpdate.getParams());
        
        String value = kColumnAllowedToSetUpdate.getSqlToSet();//EXCLUDED
        
        if (value.contains(value))
        
        this.kQueryInsertIntoData.sb.append(kColumn.name).append(" = ").append(kColumnAllowedToSetUpdate.getSqlToSet());
    }
    
    private void process(
        final KColumn kColumn,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kColumn, "kColumn");
        KUtils.assertNotNull(kQuery, "kQuery");
        
        if (this.kQueryInsertIntoData.setValuesAdded == 0) {
            this.kQueryInsertIntoData.sb.append(" SET ");
        } else {
            this.kQueryInsertIntoData.sb.append(", ");
        }
        
        this.kQueryInsertIntoData.setValuesAdded++;
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        this.kQueryInsertIntoData.params.addAll(subQuery.params);
        this.kQueryInsertIntoData.sb.append(kColumn.name).append(" = (").append(subQuery.sb).append(")");
    }
    
    public int execute() {
        return super.executeSingle();
    }
}