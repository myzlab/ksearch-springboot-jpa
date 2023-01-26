package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;
import java.util.ArrayList;
import java.util.List;

public class KValNumberField extends KBaseValField implements KColumnAllowedToSetUpdate, KColumnAllowedToSelect, KColumnAllowedToReturning {
    
    protected KValNumberField() {
        super();
    }
    
    protected KValNumberField(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    protected KValNumberField(
        final Number val
    ) {
        super(val);
    }
    
    public KColumn add(
        final KColumn kColumn
    ) {
        return KFunction.add(this, kColumn);
    }
    
    public KValNumberField add(
        final KValNumberField kValNumberField
    ) {
        return KFunction.add(this, kValNumberField);
    }
    
    public KValNumberField add(
        final Number number
    ) {
        return KFunction.add(this, number);
    }
    
    public KColumn div(
        final KColumn kColumn
    ) {
        return KFunction.div(this, kColumn);
    }
    
    public KValNumberField div(
        final KValNumberField kValNumberField
    ) {
        return KFunction.div(this, kValNumberField);
    }
    
    public KValNumberField div(
        final Number number
    ) {
        return KFunction.div(this, number);
    }
    
    public KColumn sub(
        final KColumn kColumn
    ) {
        return KFunction.sub(this, kColumn);
    }
    
    public KValNumberField sub(
        final KValNumberField kValNumberField
    ) {
        return KFunction.sub(this, kValNumberField);
    }
    
    public KValNumberField sub(
        final Number number
    ) {
        return KFunction.sub(this, new KValNumberField(number));
    }
    
    public KColumn mod(
        final KColumn kColumn
    ) {
        return KFunction.mod(this, kColumn);
    }
    
    public KValNumberField mod(
        final KValNumberField kValNumberField
    ) {
        return KFunction.mod(this, kValNumberField);
    }
    
    public KValNumberField mod(
        final Number number
    ) {
        return KFunction.mod(this, new KValNumberField(number));
    }
    
    public KColumn mul(
        final KColumn kColumn
    ) {
        return KFunction.mul(this, kColumn);
    }
    
    public KValNumberField mul(
        final KValNumberField kValNumberField
    ) {
        return KFunction.mul(this, kValNumberField);
    }
    
    public KValNumberField mul(
        final Number number
    ) {
        return KFunction.mul(this, new KValNumberField(number));
    }
    
    public KColumnOrdered asc() {
        return new KColumnOrdered(sb, params, closed, 1);
    }
    
    public KColumnOrdered desc() {
        return new KColumnOrdered(sb, params, closed, -1);
    }
    
    @Override
    protected KValNumberField cloneMe() {
        return new KValNumberField(this.sb, new ArrayList<>(this.params), this.closed);
    }

    @Override
    public String getSqlToSet() {
        return this.sb.toString();
    }

    @Override
    public List<Object> getParams() {
        return this.params;
    }
    
    @Override
    public KBaseColumn getKBaseColumn() {
        return this;
    }
    
    @Override
    public String getSqlToReturning() {
        return sb.toString();
    }
}
