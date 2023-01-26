package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import java.util.List;

public class KInsertIntoFilled extends KQueryInsertInto {

    private KInsertIntoFilled(
        final KExecutor kExecutor
    ) {
        super(kExecutor);
    }
 
    private KInsertIntoFilled(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KQuery kQuery
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process(kQuery);
    }
    
    private KInsertIntoFilled(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KValues kValues
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process(kValues);
    }
    
    protected static KInsertIntoFilled getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KQuery kQuery
    ) {
        return new KInsertIntoFilled(kExecutor, kQueryInsertIntoData, kQuery);
    }
    
    protected static KInsertIntoFilled getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KValues kValues
    ) {
        return new KInsertIntoFilled(kExecutor, kQueryInsertIntoData, kValues);
    }
    
    public KInsertIntoOnConflict onConflict() {
        return KInsertIntoOnConflict.getInstance(this.k, this.kQueryInsertIntoData);
    }
    
    public KReturningInsertInto returning(
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        return KReturningInsertInto.getInstance(this.k, this.kQueryInsertIntoData, kColumnsAllowedToReturning);
    }
    
    private void process(
        final KValues kValues
    ) {
        this.kQueryInsertIntoData.sb.append(" VALUES ");
        
        final List<List<Object>> AllValues = kValues.values;
            
        for (int i = 0; i < AllValues.size(); i++) {
            final List<Object> values = AllValues.get(i);
            
            if (i > 0) {
                this.kQueryInsertIntoData.sb.append(", ");
            }

            this.kQueryInsertIntoData.sb.append("(");

            for (int j = 0; j < values.size(); j++) {
                if (j > 0) {
                    this.kQueryInsertIntoData.sb.append(", ");
                }
                
                if (values.get(j) instanceof KRaw) {
                    this.kQueryInsertIntoData.sb.append(((KRaw) values.get(j)).content);
                } else {
                    this.kQueryInsertIntoData.sb.append("?");
                    this.kQueryInsertIntoData.params.add(values.get(j));
                }
            }

            this.kQueryInsertIntoData.sb.append(")");
        }
    }
    
    private void process(
        final KQuery kQuery
    ) {
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        this.kQueryInsertIntoData.params.addAll(subQuery.params);
        this.kQueryInsertIntoData.sb.append(" ").append(subQuery.sb);
    }
    
    public int execute() {
        return super.executeSingle();
    }
}

