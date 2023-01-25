package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;
import java.util.ArrayList;

public class KSetUpdate extends KQueryUpdate {

    private KSetUpdate() {
        super();
    }
    
    private KSetUpdate(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KColumn kColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        super(kQueryUpdateData, kExecutor);
        
        this.process(kColumn, kColumnAllowedToSetUpdate);
    }
    
    private KSetUpdate(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KColumn kColumn,
        final KQuery kQuery
    ) {
        super(kQueryUpdateData, kExecutor);
        
        this.process(kColumn, kQuery);
    }
    
    protected static KSetUpdate getInstance(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KColumn kColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        return new KSetUpdate(kExecutor, kQueryUpdateData, kColumn, kColumnAllowedToSetUpdate);
    }
    
    protected static KSetUpdate getInstance(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KColumn kColumn,
        final KQuery kQuery
    ) {
        return new KSetUpdate(kExecutor, kQueryUpdateData, kColumn, kQuery);
    }
    
    public KFromUpdate from(
        final KTable kTable
    ) {
        return KFromUpdate.getInstance(this.k, this.kQueryUpdateData, kTable);
    }
    
    public KFromUpdate from(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        return KFromUpdate.getInstance(this.k, this.kQueryUpdateData, new KTable(null, kRaw.content, null));
    }
    
    public KFromUpdate from(
        final KCommonTableExpressionFilled kCommonTableExpressionFilled
    ) {
        return KFromUpdate.getInstance(this.k, this.kQueryUpdateData, new KTable(null, kCommonTableExpressionFilled.name, kCommonTableExpressionFilled.alias));
    }
    
    public KReturningUpdate returning(
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        return KReturningUpdate.getInstance(this.k, this.kQueryUpdateData, kColumnsAllowedToReturning);
    }
    
    public KSetUpdate set(
        final KColumn kColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        KUtils.assertNotNull(kColumnAllowedToSetUpdate, "kColumnAllowedToSetUpdate");
        
        this.process(kColumn, kColumnAllowedToSetUpdate);
        
        return this;
    }
    
    public KSetUpdate set(
        final KColumn kColumn,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        this.process(kColumn, kQuery);
        
        return this;
    }
    
    public KSetUpdate set(
        final KColumn kColumn,
        final Object object
    ) {
        KUtils.assertNotNull(object, "object");
        
        final KColumn kColumnValue = new KColumn(new StringBuilder("?"), new ArrayList() {{
            add(object);
        }}, false);
        
        this.process(kColumn, kColumnValue);
        
        return this;
    }
    
    public KWhereUpdate where(
        final KCondition kCondition
    ) {
        return KWhereUpdate.getInstance(this.k, this.kQueryUpdateData, kCondition);
    }
    
    public KWhereUpdate where(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kCondition = new KCondition(kRaw.content);
        
        return KWhereUpdate.getInstance(this.k, this.kQueryUpdateData, kCondition);
    }
    
    public int execute() {
        return super.executeSingle();
    }
    
    private void process(
        final KColumn kColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        KUtils.assertNotNull(kColumn, "kColumn");
        KUtils.assertNotNull(kColumnAllowedToSetUpdate, "kColumnAllowedToSetUpdate");
        
        if (this.kQueryUpdateData.setValuesAdded == 0) {
            this.kQueryUpdateData.sb.append(" SET ");
        } else {
            this.kQueryUpdateData.sb.append(", ");
        }
        
        this.kQueryUpdateData.setValuesAdded++;
        
        this.kQueryUpdateData.params.addAll(kColumnAllowedToSetUpdate.getParams());
        this.kQueryUpdateData.sb.append(kColumn.name).append(" = ").append(kColumnAllowedToSetUpdate.getSqlToSet());
    }
    
    private void process(
        final KColumn kColumn,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kColumn, "kColumn");
        KUtils.assertNotNull(kQuery, "kQuery");
        
        if (this.kQueryUpdateData.setValuesAdded == 0) {
            this.kQueryUpdateData.sb.append(" SET ");
        } else {
            this.kQueryUpdateData.sb.append(", ");
        }
        
        this.kQueryUpdateData.setValuesAdded++;
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        this.kQueryUpdateData.params.addAll(subQuery.params);
        this.kQueryUpdateData.sb.append(kColumn.name).append(" = (").append(subQuery.sb).append(")");
    }
}