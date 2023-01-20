package com.myzlab.k;

import static com.myzlab.k.KFunction.*;
import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KQueryAllowedToCombining;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import com.myzlab.k.optional.KOptionalLong;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KWindow extends KQuery implements KQueryAllowedToCombining {
    
    final List<KWindowDefinitionAllowedToWindow> kWindowDefinitionsAllowedToWindow = new ArrayList<>();
    
    private KWindow(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KWindowDefinitionAllowedToWindow... KWindowDefinitionsAllowedToWindow
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
        
        KUtils.assertNotNull(KWindowDefinitionsAllowedToWindow, "KWindowDefinitionsAllowedToWindow");
        
        this.kWindowDefinitionsAllowedToWindow.addAll(Arrays.asList(KWindowDefinitionsAllowedToWindow));
    }
    
    protected static KWindow getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KWindowDefinitionAllowedToWindow... KWindowDefinitionsAllowedToWindow
    ) {
        return new KWindow(kExecutor, kSpecialFunctions, kQueryData, KWindowDefinitionsAllowedToWindow);
    }
    
    public KTable as(
        final String alias
    ) {
        return table(this, alias);
    }
    
    public KWindow window(
        final KWindowDefinitionAllowedToWindow... KWindowDefinitionsAllowedToWindow
    ) {
        KUtils.assertNotNull(KWindowDefinitionsAllowedToWindow, "KWindowDefinitionsAllowedToWindow");
        
        this.kWindowDefinitionsAllowedToWindow.addAll(Arrays.asList(KWindowDefinitionsAllowedToWindow));
        
        return this;
    }

    public KCombining union(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        this.buildWindow();
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "UNION", false);
    }
    
    public KCombining unionAll(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        this.buildWindow();
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "UNION", true);
    }
    
    public KCombining intersect(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        this.buildWindow();
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "INTERSECT", false);
    }
    
    public KCombining intersectAll(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        this.buildWindow();
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "INTERSECT", true);
    }
    
    public KCombining except(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        this.buildWindow();
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "EXCEPT", false);
    }
    
    public KCombining exceptAll(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        this.buildWindow();
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "EXCEPT", true);
    }
    
    public KOrderBy orderBy(
        final KColumnAllowedToOrderBy... kColumnsAllowedToOrderBy
    ) {
        this.buildWindow();
        
        return KOrderBy.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kColumnsAllowedToOrderBy);
    }
    
    public KLimit limit(
        final int count
    ) {
        this.buildWindow();
        
        return KLimit.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, count);
    }
    
    public KLimit limit(
        final long count
    ) {
        return KLimit.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, count);
    }
    
    public KLimit limit(
        final KOptionalLong kOptionalLong
    ) {
        this.buildWindow();
        
        if (!kOptionalLong.isPresent()) {
            return KLimit.getInstance(this.k, this.kSpecialFunctions, this.kQueryData);
        }
        
        return KLimit.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kOptionalLong.get());
    }
    
    public KOffset offset(
        final int start
    ) {
        this.buildWindow();
        
        return KOffset.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, start);
    }
    
    public KOffset offset(
        final long start
    ) {
        this.buildWindow();
        
        return KOffset.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, start);
    }
    
    public KOffset offset(
        final KOptionalLong kOptionalLong
    ) {
        this.buildWindow();
        
        if (!kOptionalLong.isPresent()) {
            return KOffset.getInstance(this.k, this.kSpecialFunctions, this.kQueryData);
        }
        
        return KOffset.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kOptionalLong.get());
    }
    
    public KFetch fetch(
        final int rowCount
    ) {
        this.buildWindow();
        
        return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, rowCount);
    }
    
    public KFetch fetch(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData);
        }
        
        return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kOptionalLong.get());
    }
    
    private void buildWindow() {
        KQueryUtils.buildWindow(
            this.kQueryData, 
            this.kWindowDefinitionsAllowedToWindow
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onBuildWindow(this.kWindowDefinitionsAllowedToWindow);
        }
    }
    
    @Override
    public <T> T single(
        final Class<T> clazz
    ) {
        this.buildWindow();
        
        return super.single(clazz);
    }
    
    @Override
    public <T extends KRow> KCollection<T> multiple(
        final Class<T> clazz
    ) {
        this.buildWindow();
        
        return super.multiple(clazz);
    }
    
    @Override
    public KQueryData generateSubQueryData() {
        final KQueryData newKQueryData = this.kQueryData.cloneMe();
        
        KQueryUtils.buildWindow(
            newKQueryData, 
            this.kWindowDefinitionsAllowedToWindow
        );
        
        return newKQueryData;
    }
}
