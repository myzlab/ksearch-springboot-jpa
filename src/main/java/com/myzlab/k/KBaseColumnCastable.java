package com.myzlab.k;

import com.myzlab.k.optional.KOptionalArrayObject;
import com.myzlab.k.optional.KOptionalBoolean;
import com.myzlab.k.optional.KOptionalCollection;
import com.myzlab.k.optional.KOptionalKColumn;
import com.myzlab.k.optional.KOptionalKValNumberField;
import com.myzlab.k.optional.KOptionalNumber;
import com.myzlab.k.optional.KOptionalString;
import com.myzlab.k.optional.KOptionalKValTextField;
import com.myzlab.k.optional.KOptionalLocalDate;
import com.myzlab.k.optional.KOptionalLocalDateTime;
import com.myzlab.k.optional.KOptionalLong;
import com.myzlab.k.optional.KOptionalUuid;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public abstract class KBaseColumnCastable extends KBaseColumn {
    
    protected KBaseColumnCastable() {
        super();
    }
    
    protected KBaseColumnCastable(
        final boolean closed
    ) {
        super(closed);
    }
    
    protected KBaseColumnCastable(
        final boolean closed,
        final String name,
        final Class type,
        final KTable kTable
    ) {
        super(closed, name, type, kTable);
    }
    
    protected KBaseColumnCastable(
        final boolean closed,
        final String name,
        final Class type,
        final KTable kTable,
        final KColumnDataType columnDataType
    ) {
        super(closed, name, type, kTable, columnDataType);
    }
    
    protected KBaseColumnCastable(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    protected KBaseColumnCastable(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed,
        final String name,
        final Class type,
        final KTable kTable
    ) {
        super(sb, params, closed, name, type, kTable);
        
        KUtils.assertNotNull(kTable, "kTable");
        KUtils.assertNotNull(name, "name");
        KUtils.assertNotNull(type, "type");
    }
    
    protected KBaseColumnCastable(
        final StringBuilder sb
    ) {
        super(sb);
    }
    
    protected KBaseColumnCastable(
        final String val
    ) {
        super(val);
    }
    
    protected KBaseColumnCastable(
        final Boolean val
    ) {
        super(val, true);
    }
    
    protected KBaseColumnCastable(
        final Number val
    ) {
        super(val);
    }
    
    protected KBaseColumnCastable(
        final UUID val
    ) {
        super(val);
    }
    
    protected KBaseColumnCastable(
        final LocalDate val
    ) {
        super(val);
    }
    
    protected KBaseColumnCastable(
        final LocalDateTime val
    ) {
        super(val);
    }
    
    public KAliasedColumn as(
        final String alias
    ) {
        return KFunction.as(this, alias);
    }
    
    public KCondition between(
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    ) {
        return KCondition.bt(this, kColumnLow, kColumnHigh);
    }
    
    public KCondition between(
        final KValNumberField kValNumberFieldLow,
        final KValNumberField kValNumberFieldHigh
    ) {
        return KCondition.bt(this, kValNumberFieldLow, kValNumberFieldHigh);
    }
    
    public KCondition between(
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    ) {
        return KCondition.bt(this, kValTextFieldLow, kValTextFieldHigh);
    }
    
    public KCondition between(
        final Number numberLow,
        final Number numberHigh
    ) {
        return KCondition.bt(this, KFunction.val(numberLow), KFunction.val(numberHigh));
    }
    
    public KCondition between(
        final String valueLow,
        final String valueHigh
    ) {
        return KCondition.bt(this, KFunction.val(valueLow), KFunction.val(valueHigh));
    }
    
    public KCondition bt(
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    ) {
        return KCondition.bt(this, kColumnLow, kColumnHigh);
    }
    
    public KCondition bt(
        final KValNumberField kValNumberFieldLow,
        final KValNumberField kValNumberFieldHigh
    ) {
        return KCondition.bt(this, kValNumberFieldLow, kValNumberFieldHigh);
    }
    
    public KCondition bt(
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    ) {
        return KCondition.bt(this, kValTextFieldLow, kValTextFieldHigh);
    }
    
    public KCondition bt(
        final Number numberLow,
        final Number numberHigh
    ) {
        return KCondition.bt(this, KFunction.val(numberLow), KFunction.val(numberHigh));
    }
    
    public KCondition bt(
        final String valueLow,
        final String valueHigh
    ) {
        return KCondition.bt(this, KFunction.val(valueLow), KFunction.val(valueHigh));
    }
    
    public KCondition bt(
        final LocalDate valueLow,
        final LocalDate valueHigh
    ) {
        return KCondition.bt(this, KFunction.val(valueLow), KFunction.val(valueHigh));
    }
    
    public KCondition bt(
        final LocalDateTime valueLow,
        final LocalDateTime valueHigh
    ) {
        return KCondition.bt(this, KFunction.val(valueLow), KFunction.val(valueHigh));
    }
    
    public KColumn cast(
        final KDataType kDataType    
    ) {
        return KFunction.cast(this, kDataType);
    }
    
    public KCondition eq(
        final KColumn kColumn
    ) {
        return KCondition.eq(this, kColumn);
    }
    
    public KCondition eq(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.eq(kOptionalKColumn.get());
    }
    
    public KCondition eq(
        final KValNumberField kValNumberField
    ) {
        return KCondition.eq(this, kValNumberField);
    }
    
    public KCondition eq(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.eq(kOptionalKValNumberField.get());
    }
    
    public KCondition eq(
        final KValTextField kValTextField
    ) {
        return KCondition.eq(this, kValTextField);
    }
    
    public KCondition eq(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.eq(kOptionalKValTextField.get());
    }
    
    public KCondition eq(
        final Number number
    ) {
        return KCondition.eq(this, KFunction.val(number));
    }
    
    public KCondition eq(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.eq(kOptionalNumber.get());
    }
    
    public KCondition eq(
        final String value
    ) {
        return KCondition.eq(this, KFunction.val(value));
    }
    
    public KCondition eq(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.eq(kOptionalString.get());
    }
    
    public KCondition eq(
        final Boolean value
    ) {
        return KCondition.eq(this, KFunction.val(value));
    }
    
    public KCondition eq(
        final KOptionalBoolean kOptionalBoolean
    ) {
        if (!kOptionalBoolean.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.eq(kOptionalBoolean.get());
    }
    
    public KCondition eq(
        final LocalDate localDate
    ) {
        return KCondition.eq(this, KFunction.val(localDate));
    }
    
    public KCondition eq(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.eq(kOptionalKValLocalDate.get());
    }
    
    public KCondition eq(
        final LocalDateTime localDateTime
    ) {
        return KCondition.eq(this, KFunction.val(localDateTime));
    }
    
    public KCondition eq(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.eq(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition eq(
        final UUID uuid
    ) {
        return KCondition.eq(this, KFunction.val(uuid));
    }
    
    public KCondition eq(
        final KOptionalUuid kOptionalUuid
    ) {
        if (!kOptionalUuid.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.eq(kOptionalUuid.get());
    }
    
    public KCondition eq(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.eq(kOptionalLong.get());
    }
    
    public KCondition eq(
        final KQuery kQuery
    ) {
        return KCondition.eq(this, kQuery);
    }
    
    public KCondition eq(
        final Object o
    ) {
        return KCondition.eq(this, o);
    }
    
    public KCondition equal(
        final KColumn kColumn
    ) {
        return KCondition.eq(this, kColumn);
    }
    
    public KCondition equal(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.eq(this, kOptionalKColumn.get());
    }
    
    public KCondition equal(
        final KValNumberField kValNumberField
    ) {
        return KCondition.eq(this, kValNumberField);
    }
    
    public KCondition equal(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.eq(this, kOptionalKValNumberField.get());
    }
    
    public KCondition equal(
        final KValTextField kValTextField
    ) {
        return KCondition.eq(this, kValTextField);
    }
    
    public KCondition equal(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.eq(this, kOptionalKValTextField.get());
    }
    
    public KCondition equal(
        final Number number
    ) {
        return KCondition.eq(this, KFunction.val(number));
    }
    
    public KCondition equal(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.eq(this, KFunction.val(kOptionalNumber.get()));
    }
    
    public KCondition equal(
        final String value
    ) {
        return KCondition.eq(this, KFunction.val(value));
    }
    
    public KCondition equal(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.eq(this, KFunction.val(kOptionalString.get()));
    }
    
    public KCondition equal(
        final Boolean value
    ) {
        return KCondition.eq(this, KFunction.val(value));
    }
    
    public KCondition equal(
        final KOptionalBoolean kOptionalBoolean
    ) {
        if (!kOptionalBoolean.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.eq(this, KFunction.val(kOptionalBoolean.get()));
    }
    
    public KCondition equal(
        final LocalDate localDate
    ) {
        return KCondition.eq(this, KFunction.val(localDate));
    }
    
    public KCondition equal(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.eq(this, KFunction.val(kOptionalKValLocalDate.get()));
    }
    
    public KCondition equal(
        final LocalDateTime localDateTime
    ) {
        return KCondition.eq(this, KFunction.val(localDateTime));
    }
    
    public KCondition equal(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.eq(this, KFunction.val(kOptionalKValLocalDateTime.get()));
    }
    
    public KCondition equal(
        final UUID uuid
    ) {
        return KCondition.eq(this, KFunction.val(uuid));
    }
    
    public KCondition equal(
        final KOptionalUuid kOptionalUuid
    ) {
        if (!kOptionalUuid.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.eq(this, KFunction.val(kOptionalUuid.get()));
    }
    
    public KCondition equal(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.eq(this, KFunction.val(kOptionalLong.get()));
    }
    
    public KCondition equal(
        final KQuery kQuery
    ) {
        return KCondition.eq(this, kQuery);
    }
    
    public KCondition greaterThan(
        final KColumn kColumn
    ) {
        return KCondition.gt(this, kColumn);
    }
    
    public KCondition greaterThan(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalKColumn.get());
    }
    
    public KCondition greaterThan(
        final KValNumberField kValNumberField
    ) {
        return KCondition.gt(this, kValNumberField);
    }
    
    public KCondition greaterThan(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalKValNumberField.get());
    }
    
    public KCondition greaterThan(
        final KValTextField kValTextField
    ) {
        return KCondition.gt(this, kValTextField);
    }
    
    public KCondition greaterThan(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalKValTextField.get());
    }
    
    public KCondition greaterThan(
        final Number number
    ) {
        return KCondition.gt(this, KFunction.val(number));
    }
    
    public KCondition greaterThan(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalNumber.get());
    }
    
    public KCondition greaterThan(
        final String value
    ) {
        return KCondition.gt(this, KFunction.val(value));
    }
    
    public KCondition greaterThan(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalString.get());
    }
    
    public KCondition greaterThan(
        final LocalDate localDate
    ) {
        return KCondition.gt(this, KFunction.val(localDate));
    }
    
    public KCondition greaterThan(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalKValLocalDate.get());
    }
    
    public KCondition greaterThan(
        final LocalDateTime localDateTime
    ) {
        return KCondition.gt(this, KFunction.val(localDateTime));
    }
    
    public KCondition greaterThan(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition greaterThan(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalLong.get());
    }
    
    public KCondition greaterThan(
        final KQuery kQuery
    ) {
        return KCondition.gt(this, kQuery);
    }
    
    public KCondition greaterThanOrEqualTo(
        final KColumn kColumn
    ) {
        return KCondition.gte(this, kColumn);
    }
    
    public KCondition greaterThanOrEqualTo(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalKColumn.get());
    }
    
    public KCondition greaterThanOrEqualTo(
        final KValNumberField kValNumberField
    ) {
        return KCondition.gte(this, kValNumberField);
    }
    
    public KCondition greaterThanOrEqualTo(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalKValNumberField.get());
    }
    
    public KCondition greaterThanOrEqualTo(
        final KValTextField kValTextField
    ) {
        return KCondition.gte(this, kValTextField);
    }
    
    public KCondition greaterThanOrEqualTo(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalKValTextField.get());
    }
    
    public KCondition greaterThanOrEqualTo(
        final Number number
    ) {
        return KCondition.gte(this, KFunction.val(number));
    }
    
    public KCondition greaterThanOrEqualTo(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalNumber.get());
    }
    
    public KCondition greaterThanOrEqualTo(
        final String value
    ) {
        return KCondition.gte(this, KFunction.val(value));
    }
    
    public KCondition greaterThanOrEqualTo(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalString.get());
    }
    
    public KCondition greaterThanOrEqualTo(
        final LocalDate localDate
    ) {
        return KCondition.gte(this, KFunction.val(localDate));
    }
    
    public KCondition greaterThanOrEqualTo(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalKValLocalDate.get());
    }
    
    public KCondition greaterThanOrEqualTo(
        final LocalDateTime localDateTime
    ) {
        return KCondition.gte(this, KFunction.val(localDateTime));
    }
    
    public KCondition greaterThanOrEqualTo(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition greaterThanOrEqualTo(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalLong.get());
    }
    
    public KCondition greaterThanOrEqualTo(
        final KQuery kQuery
    ) {
        return KCondition.gte(this, kQuery);
    }
    
    public KCondition gt(
        final KColumn kColumn
    ) {
        return KCondition.gt(this, kColumn);
    }
    
    public KCondition gt(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalKColumn.get());
    }
    
    public KCondition gt(
        final KValNumberField kValNumberField
    ) {
        return KCondition.gt(this, kValNumberField);
    }
    
    public KCondition gt(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalKValNumberField.get());
    }
    
    public KCondition gt(
        final KValTextField kValTextField
    ) {
        return KCondition.gt(this, kValTextField);
    }
    
    public KCondition gt(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalKValTextField.get());
    }
    
    public KCondition gt(
        final Number number
    ) {
        return KCondition.gt(this, KFunction.val(number));
    }
    
    public KCondition gt(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalNumber.get());
    }
    
    public KCondition gt(
        final String value
    ) {
        return KCondition.gt(this, KFunction.val(value));
    }
    
    public KCondition gt(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalString.get());
    }
    
    public KCondition gt(
        final LocalDate localDate
    ) {
        return KCondition.gt(this, KFunction.val(localDate));
    }
    
    public KCondition gt(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalKValLocalDate.get());
    }
    
    public KCondition gt(
        final LocalDateTime localDateTime
    ) {
        return KCondition.gt(this, KFunction.val(localDateTime));
    }
    
    public KCondition gt(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition gt(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gt(kOptionalLong.get());
    }
    
    public KCondition gt(
        final KQuery kQuery
    ) {
        return KCondition.gt(this, kQuery);
    }
    
    public KCondition gte(
        final KColumn kColumn
    ) {
        return KCondition.gte(this, kColumn);
    }
    
    public KCondition gte(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalKColumn.get());
    }
    
    public KCondition gte(
        final KValNumberField kValNumberField
    ) {
        return KCondition.gte(this, kValNumberField);
    }
    
    public KCondition gte(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalKValNumberField.get());
    }
    
    public KCondition gte(
        final KValTextField kValTextField
    ) {
        return KCondition.gte(this, kValTextField);
    }
    
    public KCondition gte(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalKValTextField.get());
    }
    
    public KCondition gte(
        final Number number
    ) {
        return KCondition.gte(this, KFunction.val(number));
    }
    
    public KCondition gte(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalNumber.get());
    }
    
    public KCondition gte(
        final String value
    ) {
        return KCondition.gte(this, KFunction.val(value));
    }
    
    public KCondition gte(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalString.get());
    }
    
    public KCondition gte(
        final LocalDate localDate
    ) {
        return KCondition.gte(this, KFunction.val(localDate));
    }
    
    public KCondition gte(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalKValLocalDate.get());
    }
    
    public KCondition gte(
        final LocalDateTime localDateTime
    ) {
        return KCondition.gte(this, KFunction.val(localDateTime));
    }
    
    public KCondition gte(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition gte(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.gte(kOptionalLong.get());
    }
    
    public KCondition gte(
        final KQuery kQuery
    ) {
        return KCondition.gte(this, kQuery);
    }
    
    public KCondition in(
        final Object[] values
    ) {
        return KCondition.in(this, Arrays.asList(values));
    }
    
    public KCondition in(
        final KOptionalArrayObject kOptionalArrayObject
    ) {
        if (!kOptionalArrayObject.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.in(kOptionalArrayObject.get());
    }
    
    public KCondition in(
        final Collection values
    ) {
        return KCondition.in(this, values);
    }
    
    public KCondition in(
        final KColumn kColumn
    ) {
        return KCondition.in(this, kColumn);
    }
    
    public KCondition in(
        final KOptionalCollection kOptionalCollection
    ) {
        if (!kOptionalCollection.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.in(kOptionalCollection.get());
    }
    
    public KCondition isFalse() {
        return KCondition.isFalse(this);
    }
    
    public KCondition isNotFalse() {
        return KCondition.isNotFalse(this);
    }
    
    public KCondition isNotNull() {
        return KCondition.isNotNull(this);
    }
    
    public KCondition isNull() {
        return KCondition.isNull(this);
    }
    
    public KCondition isNotTrue() {
        return KCondition.isNotTrue(this);
    }
    
    public KCondition isTrue() {
        return KCondition.isTrue(this);
    }
    
    public KCondition isUnknown() {
        return KCondition.isUnknown(this);
    }
    
    public KCondition isNotUnknown() {
        return KCondition.isNotUnknown(this);
    }
    
    public KCondition lessThan(
        final KColumn kColumn
    ) {
        return KCondition.lt(this, kColumn);
    }
    
    public KCondition lessThan(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalKColumn.get());
    }
    
    public KCondition lessThan(
        final KValNumberField kValNumberField
    ) {
        return KCondition.lt(this, kValNumberField);
    }
    
    public KCondition lessThan(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalKValNumberField.get());
    }
    
    public KCondition lessThan(
        final KValTextField kValTextField
    ) {
        return KCondition.lt(this, kValTextField);
    }
    
    public KCondition lessThan(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalKValTextField.get());
    }
    
    public KCondition lessThan(
        final Number number
    ) {
        return KCondition.lt(this, KFunction.val(number));
    }
    
    public KCondition lessThan(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalNumber.get());
    }
    
    public KCondition lessThan(
        final String value
    ) {
        return KCondition.lt(this, KFunction.val(value));
    }
    
    public KCondition lessThan(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalString.get());
    }
    
    public KCondition lessThan(
        final LocalDate localDate
    ) {
        return KCondition.lt(this, KFunction.val(localDate));
    }
    
    public KCondition lessThan(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalKValLocalDate.get());
    }
    
    public KCondition lessThan(
        final LocalDateTime localDateTime
    ) {
        return KCondition.lt(this, KFunction.val(localDateTime));
    }
    
    public KCondition lessThan(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition lessThan(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalLong.get());
    }
    
    public KCondition lessThan(
        final KQuery kQuery
    ) {
        return KCondition.lt(this, kQuery);
    }
    
    public KCondition lessThanOrEqualTo(
        final KColumn kColumn
    ) {
        return KCondition.lte(this, kColumn);
    }
    
    public KCondition lessThanOrEqualTo(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalKColumn.get());
    }
    
    public KCondition lessThanOrEqualTo(
        final KValNumberField kValNumberField
    ) {
        return KCondition.lte(this, kValNumberField);
    }
    
    public KCondition lessThanOrEqualTo(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalKValNumberField.get());
    }
    
    public KCondition lessThanOrEqualTo(
        final KValTextField kValTextField
    ) {
        return KCondition.lte(this, kValTextField);
    }
    
    public KCondition lessThanOrEqualTo(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalKValTextField.get());
    }
    
    public KCondition lessThanOrEqualTo(
        final Number number
    ) {
        return KCondition.lte(this, KFunction.val(number));
    }
    
    public KCondition lessThanOrEqualTo(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalNumber.get());
    }
    
    public KCondition lessThanOrEqualTo(
        final String value
    ) {
        return KCondition.lte(this, KFunction.val(value));
    }
    
    public KCondition lessThanOrEqualTo(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalString.get());
    }
    
    public KCondition lessThanOrEqualTo(
        final LocalDate localDate
    ) {
        return KCondition.lte(this, KFunction.val(localDate));
    }
    
    public KCondition lessThanOrEqualTo(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalKValLocalDate.get());
    }
    
    public KCondition lessThanOrEqualTo(
        final LocalDateTime localDateTime
    ) {
        return KCondition.lte(this, KFunction.val(localDateTime));
    }
    
    public KCondition lessThanOrEqualTo(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition lessThanOrEqualTo(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalLong.get());
    }
    
    public KCondition lessThanOrEqualTo(
        final KQuery kQuery
    ) {
        return KCondition.lte(this, kQuery);
    }
    
    public KCondition lt(
        final KColumn kColumn
    ) {
        return KCondition.lt(this, kColumn);
    }
    
    public KCondition lt(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalKColumn.get());
    }
    
    public KCondition lt(
        final KValNumberField kValNumberField
    ) {
        return KCondition.lt(this, kValNumberField);
    }
    
    public KCondition lt(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalKValNumberField.get());
    }
    
    public KCondition lt(
        final KValTextField kValTextField
    ) {
        return KCondition.lt(this, kValTextField);
    }
    
    public KCondition lt(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalKValTextField.get());
    }
    
    public KCondition lt(
        final Number number
    ) {
        return KCondition.lt(this, KFunction.val(number));
    }
    
    public KCondition lt(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalNumber.get());
    }
    
    public KCondition lt(
        final String value
    ) {
        return KCondition.lt(this, KFunction.val(value));
    }
    
    public KCondition lt(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalString.get());
    }
    
    public KCondition lt(
        final LocalDate localDate
    ) {
        return KCondition.lt(this, KFunction.val(localDate));
    }
    
    public KCondition lt(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalKValLocalDate.get());
    }
    
    public KCondition lt(
        final LocalDateTime localDateTime
    ) {
        return KCondition.lt(this, KFunction.val(localDateTime));
    }
    
    public KCondition lt(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition lt(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lt(kOptionalLong.get());
    }
    
    public KCondition lt(
        final KQuery kQuery
    ) {
        return KCondition.lt(this, kQuery);
    }
    
    public KCondition lte(
        final KColumn kColumn
    ) {
        return KCondition.lte(this, kColumn);
    }
    
    public KCondition lte(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalKColumn.get());
    }
    
    public KCondition lte(
        final KValNumberField kValNumberField
    ) {
        return KCondition.lte(this, kValNumberField);
    }
    
    public KCondition lte(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalKValNumberField.get());
    }
    
    public KCondition lte(
        final KValTextField kValTextField
    ) {
        return KCondition.lte(this, kValTextField);
    }
    
    public KCondition lte(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalKValTextField.get());
    }
    
    public KCondition lte(
        final Number number
    ) {
        return KCondition.lte(this, KFunction.val(number));
    }
    
    public KCondition lte(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalNumber.get());
    }
    
    public KCondition lte(
        final String value
    ) {
        return KCondition.lte(this, KFunction.val(value));
    }
    
    public KCondition lte(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalString.get());
    }
    
    public KCondition lte(
        final LocalDate localDate
    ) {
        return KCondition.lte(this, KFunction.val(localDate));
    }
    
    public KCondition lte(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalKValLocalDate.get());
    }
    
    public KCondition lte(
        final LocalDateTime localDateTime
    ) {
        return KCondition.lte(this, KFunction.val(localDateTime));
    }
    
    public KCondition lte(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition lte(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lte(kOptionalLong.get());
    }
    
    public KCondition lte(
        final KQuery kQuery
    ) {
        return KCondition.lte(this, kQuery);
    }
    
    public KCondition nbt(
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    ) {
        return KCondition.nbt(this, kColumnLow, kColumnHigh);
    }
    
    public KCondition nbt(
        final KValNumberField kValNumberFieldLow,
        final KValNumberField kValNumberFieldHigh
    ) {
        return KCondition.nbt(this, kValNumberFieldLow, kValNumberFieldHigh);
    }
    
    public KCondition nbt(
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    ) {
        return KCondition.nbt(this, kValTextFieldLow, kValTextFieldHigh);
    }
    
    public KCondition nbt(
        final Number numberLow,
        final Number numberHigh
    ) {
        return KCondition.nbt(this, KFunction.val(numberLow), KFunction.val(numberHigh));
    }
    
    public KCondition nbt(
        final String valueLow,
        final String valueHigh
    ) {
        return KCondition.nbt(this, KFunction.val(valueLow), KFunction.val(valueHigh));
    }
    
    public KCondition neq(
        final KColumn kColumn
    ) {
        return KCondition.neq(this, kColumn);
    }
    
    public KCondition neq(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.neq(kOptionalKColumn.get());
    }
    
    public KCondition neq(
        final KValTextField kValTextField
    ) {
        return KCondition.neq(this, kValTextField);
    }
    
    public KCondition neq(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, kOptionalKValTextField.get());
    }
    
    public KCondition neq(
        final KValNumberField kValNumberField
    ) {
        return KCondition.neq(this, kValNumberField);
    }
    
    public KCondition neq(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, kOptionalKValNumberField.get());
    }
    
    public KCondition neq(
        final Number number
    ) {
        return KCondition.neq(this, KFunction.val(number));
    }
    
    public KCondition neq(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalNumber.get()));
    }
    
    public KCondition neq(
        final String value
    ) {
        return KCondition.neq(this, KFunction.val(value));
    }
    
    public KCondition neq(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalString.get()));
    }
    
    public KCondition neq(
        final Boolean value
    ) {
        return KCondition.neq(this, KFunction.val(value));
    }
    
    public KCondition neq(
        final KOptionalBoolean kOptionalBoolean
    ) {
        if (!kOptionalBoolean.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalBoolean.get()));
    }
    
    public KCondition neq(
        final LocalDate localDate
    ) {
        return KCondition.neq(this, KFunction.val(localDate));
    }
    
    public KCondition neq(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalKValLocalDate.get()));
    }
    
    public KCondition neq(
        final LocalDateTime localDateTime
    ) {
        return KCondition.neq(this, KFunction.val(localDateTime));
    }
    
    public KCondition neq(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalKValLocalDateTime.get()));
    }
    
    public KCondition neq(
        final UUID uuid
    ) {
        return KCondition.neq(this, KFunction.val(uuid));
    }
    
    public KCondition neq(
        final KOptionalUuid kOptionalUuid
    ) {
        if (!kOptionalUuid.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalUuid.get()));
    }
    
    public KCondition neq(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalLong.get()));
    }
    
    public KCondition neq(
        final KQuery kQuery
    ) {
        return KCondition.neq(this, kQuery);
    }
    
    public KCondition ngt(
        final KColumn kColumn
    ) {
        return KCondition.ngt(this, kColumn);
    }
    
    public KCondition ngt(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalKColumn.get());
    }
    
    public KCondition ngt(
        final KValNumberField kValNumberField
    ) {
        return KCondition.ngt(this, kValNumberField);
    }
    
    public KCondition ngt(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalKValNumberField.get());
    }
    
    public KCondition ngt(
        final KValTextField kValTextField
    ) {
        return KCondition.ngt(this, kValTextField);
    }
    
    public KCondition ngt(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalKValTextField.get());
    }
    
    public KCondition ngt(
        final Number number
    ) {
        return KCondition.ngt(this, KFunction.val(number));
    }
    
    public KCondition ngt(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalNumber.get());
    }
    
    public KCondition ngt(
        final String value
    ) {
        return KCondition.ngt(this, KFunction.val(value));
    }
    
    public KCondition ngt(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalString.get());
    }
    
    public KCondition ngt(
        final LocalDate localDate
    ) {
        return KCondition.ngt(this, KFunction.val(localDate));
    }
    
    public KCondition ngt(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalKValLocalDate.get());
    }
    
    public KCondition ngt(
        final LocalDateTime localDateTime
    ) {
        return KCondition.ngt(this, KFunction.val(localDateTime));
    }
    
    public KCondition ngt(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition ngt(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalLong.get());
    }
    
    public KCondition ngt(
        final KQuery kQuery
    ) {
        return KCondition.ngt(this, kQuery);
    }
    
    public KCondition ngte(
        final KColumn kColumn
    ) {
        return KCondition.ngte(this, kColumn);
    }
    
    public KCondition ngte(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalKColumn.get());
    }
    
    public KCondition ngte(
        final KValNumberField kValNumberField
    ) {
        return KCondition.ngte(this, kValNumberField);
    }
    
    public KCondition ngte(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalKValNumberField.get());
    }
    
    public KCondition ngte(
        final KValTextField kValTextField
    ) {
        return KCondition.ngte(this, kValTextField);
    }
    
    public KCondition ngte(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalKValTextField.get());
    }
    
    public KCondition ngte(
        final Number number
    ) {
        return KCondition.ngte(this, KFunction.val(number));
    }
    
    public KCondition ngte(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalNumber.get());
    }
    
    public KCondition ngte(
        final String value
    ) {
        return KCondition.ngte(this, KFunction.val(value));
    }
    
    public KCondition ngte(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalString.get());
    }
    
    public KCondition ngte(
        final LocalDate localDate
    ) {
        return KCondition.ngte(this, KFunction.val(localDate));
    }
    
    public KCondition ngte(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalKValLocalDate.get());
    }
    
    public KCondition ngte(
        final LocalDateTime localDateTime
    ) {
        return KCondition.ngte(this, KFunction.val(localDateTime));
    }
    
    public KCondition ngte(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition ngte(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalLong.get());
    }
    
    public KCondition ngte(
        final KQuery kQuery
    ) {
        return KCondition.ngte(this, kQuery);
    }
    
    public KCondition nlt(
        final KColumn kColumn
    ) {
        return KCondition.nlt(this, kColumn);
    }
    
    public KCondition nlt(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalKColumn.get());
    }
    
    public KCondition nlt(
        final KValNumberField kValNumberField
    ) {
        return KCondition.nlt(this, kValNumberField);
    }
    
    public KCondition nlt(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalKValNumberField.get());
    }
    
    public KCondition nlt(
        final KValTextField kValTextField
    ) {
        return KCondition.nlt(this, kValTextField);
    }
    
    public KCondition nlt(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalKValTextField.get());
    }
    
    public KCondition nlt(
        final Number number
    ) {
        return KCondition.nlt(this, KFunction.val(number));
    }
    
    public KCondition nlt(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalNumber.get());
    }
    
    public KCondition nlt(
        final String value
    ) {
        return KCondition.nlt(this, KFunction.val(value));
    }
    
    public KCondition nlt(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalString.get());
    }
    
    public KCondition nlt(
        final LocalDate localDate
    ) {
        return KCondition.nlt(this, KFunction.val(localDate));
    }
    
    public KCondition nlt(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalKValLocalDate.get());
    }
    
    public KCondition nlt(
        final LocalDateTime localDateTime
    ) {
        return KCondition.nlt(this, KFunction.val(localDateTime));
    }
    
    public KCondition nlt(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition nlt(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalLong.get());
    }
    
    public KCondition nlt(
        final KQuery kQuery
    ) {
        return KCondition.nlt(this, kQuery);
    }
    
    public KCondition nlte(
        final KColumn kColumn
    ) {
        return KCondition.nlte(this, kColumn);
    }
    
    public KCondition nlte(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalKColumn.get());
    }
    
    public KCondition nlte(
        final KValNumberField kValNumberField
    ) {
        return KCondition.nlte(this, kValNumberField);
    }
    
    public KCondition nlte(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalKValNumberField.get());
    }
    
    public KCondition nlte(
        final KValTextField kValTextField
    ) {
        return KCondition.nlte(this, kValTextField);
    }
    
    public KCondition nlte(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalKValTextField.get());
    }
    
    public KCondition nlte(
        final Number number
    ) {
        return KCondition.nlte(this, KFunction.val(number));
    }
    
    public KCondition nlte(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalNumber.get());
    }
    
    public KCondition nlte(
        final String value
    ) {
        return KCondition.nlte(this, KFunction.val(value));
    }
    
    public KCondition nlte(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalString.get());
    }
    
    public KCondition nlte(
        final LocalDate localDate
    ) {
        return KCondition.nlte(this, KFunction.val(localDate));
    }
    
    public KCondition nlte(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalKValLocalDate.get());
    }
    
    public KCondition nlte(
        final LocalDateTime localDateTime
    ) {
        return KCondition.nlte(this, KFunction.val(localDateTime));
    }
    
    public KCondition nlte(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition nlte(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalLong.get());
    }
    
    public KCondition nlte(
        final KQuery kQuery
    ) {
        return KCondition.nlte(this, kQuery);
    }
    
    public KCondition notBetween(
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    ) {
        return KCondition.nbt(this, kColumnLow, kColumnHigh);
    }
    
    public KCondition notBetween(
        final KValNumberField kValNumberFieldLow,
        final KValNumberField kValNumberFieldHigh
    ) {
        return KCondition.nbt(this, kValNumberFieldLow, kValNumberFieldHigh);
    }
    
    public KCondition notBetween(
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    ) {
        return KCondition.nbt(this, kValTextFieldLow, kValTextFieldHigh);
    }
    
    public KCondition notBetween(
        final Number numberLow,
        final Number numberHigh
    ) {
        return KCondition.nbt(this, KFunction.val(numberLow), KFunction.val(numberHigh));
    }
    
    public KCondition notBetween(
        final String valueLow,
        final String valueHigh
    ) {
        return KCondition.nbt(this, KFunction.val(valueLow), KFunction.val(valueHigh));
    }
    
    public KCondition notEqual(
        final KColumn kColumn
    ) {
        return KCondition.neq(this, kColumn);
    }
    
    public KCondition notEqual(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.neq(kOptionalKColumn.get());
    }
    
    public KCondition notEqual(
        final KValTextField kValTextField
    ) {
        return KCondition.neq(this, kValTextField);
    }
    
    public KCondition notEqual(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, kOptionalKValTextField.get());
    }
    
    public KCondition notEqual(
        final KValNumberField kValNumberField
    ) {
        return KCondition.neq(this, kValNumberField);
    }
    
    public KCondition notEqual(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, kOptionalKValNumberField.get());
    }
    
    public KCondition notEqual(
        final Number number
    ) {
        return KCondition.neq(this, KFunction.val(number));
    }
    
    public KCondition notEqual(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalNumber.get()));
    }
    
    public KCondition notEqual(
        final String value
    ) {
        return KCondition.neq(this, KFunction.val(value));
    }
    
    public KCondition notEqual(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalString.get()));
    }
    
    public KCondition notEqual(
        final Boolean value
    ) {
        return KCondition.neq(this, KFunction.val(value));
    }
    
    public KCondition notEqual(
        final KOptionalBoolean kOptionalBoolean
    ) {
        if (!kOptionalBoolean.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalBoolean.get()));
    }
    
    public KCondition notEqual(
        final LocalDate localDate
    ) {
        return KCondition.neq(this, KFunction.val(localDate));
    }
    
    public KCondition notEqual(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalKValLocalDate.get()));
    }
    
    public KCondition notEqual(
        final LocalDateTime localDateTime
    ) {
        return KCondition.neq(this, KFunction.val(localDateTime));
    }
    
    public KCondition notEqual(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalKValLocalDateTime.get()));
    }
    
    public KCondition notEqual(
        final UUID uuid
    ) {
        return KCondition.neq(this, KFunction.val(uuid));
    }
    
    public KCondition notEqual(
        final KOptionalUuid kOptionalUuid
    ) {
        if (!kOptionalUuid.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalUuid.get()));
    }
    
    public KCondition notEqual(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.neq(this, KFunction.val(kOptionalLong.get()));
    }
    
    public KCondition notEqual(
        final KQuery kQuery
    ) {
        return KCondition.neq(this, kQuery);
    }
    
    public KCondition notGreaterThan(
        final KColumn kColumn
    ) {
        return KCondition.ngt(this, kColumn);
    }
    
    public KCondition notGreaterThan(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalKColumn.get());
    }
    
    public KCondition notGreaterThan(
        final KValNumberField kValNumberField
    ) {
        return KCondition.ngt(this, kValNumberField);
    }
    
    public KCondition notGreaterThan(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalKValNumberField.get());
    }
    
    public KCondition notGreaterThan(
        final KValTextField kValTextField
    ) {
        return KCondition.ngt(this, kValTextField);
    }
    
    public KCondition notGreaterThan(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalKValTextField.get());
    }
    
    public KCondition notGreaterThan(
        final Number number
    ) {
        return KCondition.ngt(this, KFunction.val(number));
    }
    
    public KCondition notGreaterThan(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalNumber.get());
    }
    
    public KCondition notGreaterThan(
        final String value
    ) {
        return KCondition.ngt(this, KFunction.val(value));
    }
    
    public KCondition notGreaterThan(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalString.get());
    }
    
    public KCondition notGreaterThan(
        final LocalDate localDate
    ) {
        return KCondition.ngt(this, KFunction.val(localDate));
    }
    
    public KCondition notGreaterThan(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalKValLocalDate.get());
    }
    
    public KCondition notGreaterThan(
        final LocalDateTime localDateTime
    ) {
        return KCondition.ngt(this, KFunction.val(localDateTime));
    }
    
    public KCondition notGreaterThan(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition notGreaterThan(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngt(kOptionalLong.get());
    }
    
    public KCondition notGreaterThan(
        final KQuery kQuery
    ) {
        return KCondition.ngt(this, kQuery);
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final KColumn kColumn
    ) {
        return KCondition.ngte(this, kColumn);
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalKColumn.get());
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final KValNumberField kValNumberField
    ) {
        return KCondition.ngte(this, kValNumberField);
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalKValNumberField.get());
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final KValTextField kValTextField
    ) {
        return KCondition.ngte(this, kValTextField);
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalKValTextField.get());
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final Number number
    ) {
        return KCondition.ngte(this, KFunction.val(number));
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalNumber.get());
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final String value
    ) {
        return KCondition.ngte(this, KFunction.val(value));
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalString.get());
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final LocalDate localDate
    ) {
        return KCondition.ngte(this, KFunction.val(localDate));
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalKValLocalDate.get());
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final LocalDateTime localDateTime
    ) {
        return KCondition.ngte(this, KFunction.val(localDateTime));
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ngte(kOptionalLong.get());
    }
    
    public KCondition notGreaterThanOrEqualTo(
        final KQuery kQuery
    ) {
        return KCondition.ngte(this, kQuery);
    }
    
    public KCondition notIn(
        final Object[] values
    ) {
        return KCondition.notIn(this, Arrays.asList(values));
    }
    
    public KCondition notIn(
        final KOptionalArrayObject kOptionalArrayObject
    ) {
        if (!kOptionalArrayObject.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.notIn(kOptionalArrayObject.get());
    }
    
    public KCondition notIn(
        final Collection values
    ) {
        return KCondition.notIn(this, values);
    }
    
    public KCondition notIn(
        final KColumn kColumn
    ) {
        return KCondition.notIn(this, kColumn);
    }
    
    public KCondition notIn(
        final KOptionalCollection kOptionalCollection
    ) {
        if (!kOptionalCollection.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.notIn(kOptionalCollection.get());
    }
    
    public KCondition notLessThan(
        final KColumn kColumn
    ) {
        return KCondition.nlt(this, kColumn);
    }
    
    public KCondition notLessThan(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalKColumn.get());
    }
    
    public KCondition notLessThan(
        final KValNumberField kValNumberField
    ) {
        return KCondition.nlt(this, kValNumberField);
    }
    
    public KCondition notLessThan(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalKValNumberField.get());
    }
    
    public KCondition notLessThan(
        final KValTextField kValTextField
    ) {
        return KCondition.nlt(this, kValTextField);
    }
    
    public KCondition notLessThan(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalKValTextField.get());
    }
    
    public KCondition notLessThan(
        final Number number
    ) {
        return KCondition.nlt(this, KFunction.val(number));
    }
    
    public KCondition notLessThan(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalNumber.get());
    }
    
    public KCondition notLessThan(
        final String value
    ) {
        return KCondition.nlt(this, KFunction.val(value));
    }
    
    public KCondition notLessThan(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalString.get());
    }
    
    public KCondition notLessThan(
        final LocalDate localDate
    ) {
        return KCondition.nlt(this, KFunction.val(localDate));
    }
    
    public KCondition notLessThan(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalKValLocalDate.get());
    }
    public KCondition notLessThan(
        final LocalDateTime localDateTime
    ) {
        return KCondition.nlt(this, KFunction.val(localDateTime));
    }
    
    public KCondition notLessThan(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalKValLocalDateTime.get());
    }
    
    
    public KCondition notLessThan(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlt(kOptionalLong.get());
    }
    
    public KCondition notLessThan(
        final KQuery kQuery
    ) {
        return KCondition.nlt(this, kQuery);
    }
    
    public KCondition notLessThanOrEqualTo(
        final KColumn kColumn
    ) {
        return KCondition.nlte(this, kColumn);
    }
    
    public KCondition notLessThanOrEqualTo(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalKColumn.get());
    }
    
    public KCondition notLessThanOrEqualTo(
        final KValNumberField kValNumberField
    ) {
        return KCondition.nlte(this, kValNumberField);
    }
    
    public KCondition notLessThanOrEqualTo(
        final KOptionalKValNumberField kOptionalKValNumberField
    ) {
        if (!kOptionalKValNumberField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalKValNumberField.get());
    }
    
    public KCondition notLessThanOrEqualTo(
        final KValTextField kValTextField
    ) {
        return KCondition.nlte(this, kValTextField);
    }
    
    public KCondition notLessThanOrEqualTo(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalKValTextField.get());
    }
    
    public KCondition notLessThanOrEqualTo(
        final Number number
    ) {
        return KCondition.nlte(this, KFunction.val(number));
    }
    
    public KCondition notLessThanOrEqualTo(
        final KOptionalNumber kOptionalNumber
    ) {
        if (!kOptionalNumber.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalNumber.get());
    }
    
    public KCondition notLessThanOrEqualTo(
        final String value
    ) {
        return KCondition.nlte(this, KFunction.val(value));
    }
    
    public KCondition notLessThanOrEqualTo(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalString.get());
    }
    
    public KCondition notLessThanOrEqualTo(
        final LocalDate localDate
    ) {
        return KCondition.nlte(this, KFunction.val(localDate));
    }
    
    public KCondition notLessThanOrEqualTo(
        final KOptionalLocalDate kOptionalKValLocalDate
    ) {
        if (!kOptionalKValLocalDate.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalKValLocalDate.get());
    }
    
    public KCondition notLessThanOrEqualTo(
        final LocalDateTime localDateTime
    ) {
        return KCondition.nlte(this, KFunction.val(localDateTime));
    }
    
    public KCondition notLessThanOrEqualTo(
        final KOptionalLocalDateTime kOptionalKValLocalDateTime
    ) {
        if (!kOptionalKValLocalDateTime.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalKValLocalDateTime.get());
    }
    
    public KCondition notLessThanOrEqualTo(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlte(kOptionalLong.get());
    }
    
    public KCondition notLessThanOrEqualTo(
        final KQuery kQuery
    ) {
        return KCondition.nlte(this, kQuery);
    }
}
