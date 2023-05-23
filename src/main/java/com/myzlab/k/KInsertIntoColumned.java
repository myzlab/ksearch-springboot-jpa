package com.myzlab.k;

import java.util.Map;

public class KInsertIntoColumned extends KQueryInsertInto {

    private KInsertIntoColumned(
        final KExecutor kExecutor
    ) {
        super(kExecutor);
    }
 
    private KInsertIntoColumned(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KTableColumn... kTableColumns
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process(kTableColumns);
    }
    
    protected static KInsertIntoColumned getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KTableColumn... kTableColumns
    ) {
        return new KInsertIntoColumned(kExecutor, kQueryInsertIntoData, kTableColumns);
    }
    
    public KInsertIntoFilled values(
        final KValues kValues
    ) {
        return KInsertIntoFilled.getInstance(this.k, this.kQueryInsertIntoData, kValues);
    }
    
    public KInsertIntoFilled values(
        final KValues kValues,
        final Map<Integer, String> castRules
    ) {
        return KInsertIntoFilled.getInstance(this.k, this.kQueryInsertIntoData, kValues, castRules);
    }
    
    public KInsertIntoFilled select(
        final KQuery kQuery
    ) {
        return KInsertIntoFilled.getInstance(this.k, this.kQueryInsertIntoData, kQuery);
    }
    
    private void process(
        final KTableColumn... kTableColumns
    ) {
        KUtils.assertNotNullNotEmpty(kTableColumns, "kTableColumns", false);
            
        this.kQueryInsertIntoData.sb.append(" (");
        

        for (int i = 0; i < kTableColumns.length; i++) {
            final KTableColumn kTableColumn = kTableColumns[i];
                    
            if (i > 0) {
                this.kQueryInsertIntoData.sb.append(", ");
            }

            this.kQueryInsertIntoData.sb.append(kTableColumn.name);
        }

        this.kQueryInsertIntoData.sb.append(")");
    }
    
}

