package com.myzlab.k;

import com.myzlab.k.optional.KOptionalKColumn;
import com.myzlab.k.optional.KOptionalKValTextField;
import com.myzlab.k.optional.KOptionalString;

public interface TextMethods {
    
    public KCondition iBetween(
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    );
    
    public KCondition iBetween(
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    );
    
    public KCondition iBetween(
        final String valueLow,
        final String valueHigh
    );
    
    public KCondition ibt(
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    );
    
    public KCondition ibt(
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    );
    
    public KCondition ibt(
        final String valueLow,
        final String valueHigh
    );

    public KCondition ieq(
        final KColumn kColumn
    );
    
    public KCondition ieq(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition ieq(
        final KValTextField kValTextField
    );
    
    public KCondition ieq(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition ieq(
        final String string
    );
    
    public KCondition ieq(
        final KOptionalString kOptionalString
    );
    
    public KCondition iEqual(
        final KColumn kColumn
    );
    
    public KCondition iEqual(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition iEqual(
        final KValTextField kValTextField
    );
    
    public KCondition iEqual(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition iEqual(
        final String string
    );
    
    public KCondition iEqual(
        final KOptionalString kOptionalString
    );
    
    public KCondition iGreaterThan(
        final KColumn kColumn
    );
    
    public KCondition iGreaterThan(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition iGreaterThan(
        final KValTextField kValTextField
    );
    
    public KCondition iGreaterThan(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition iGreaterThan(
        final String string
    );
    
    public KCondition iGreaterThan(
        final KOptionalString kOptionalString
    );
    
    public KCondition iGreaterThanOrEqualTo(
        final KColumn kColumn
    );
    
    public KCondition iGreaterThanOrEqualTo(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition iGreaterThanOrEqualTo(
        final KValTextField kValTextField
    );
    
    public KCondition iGreaterThanOrEqualTo(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition iGreaterThanOrEqualTo(
        final String string
    );
    
    public KCondition iGreaterThanOrEqualTo(
        final KOptionalString kOptionalString
    );
    
    public KCondition igt(
        final KColumn kColumn
    );
    
    public KCondition igt(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition igt(
        final KValTextField kValTextField
    );
    
    public KCondition igt(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition igt(
        final String string
    );
    
    public KCondition igt(
        final KOptionalString kOptionalString
    );
    
    public KCondition igte(
        final KColumn kColumn
    );
    
    public KCondition igte(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition igte(
        final KValTextField kValTextField
    );
    
    public KCondition igte(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition igte(
        final String string
    );
    
    public KCondition igte(
        final KOptionalString kOptionalString
    );
    
    public KCondition nilk(
        final KColumn kColumn
    );
    
    public KCondition nilk(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition nilk(
        final KValTextField kValTextField
    );
    
    public KCondition nilk(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition nilk(
        final String value
    );
    
    public KCondition nilk(
        final KOptionalString kOptionalString
    );
    
    public KCondition nilka(
        final KColumn kColumn
    );
    
    public KCondition nilka(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition nilka(
        final KValTextField kValTextField
    );
    
    public KCondition nilka(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition nilka(
        final String value
    );
    
    public KCondition nilka(
        final KOptionalString kOptionalString
    );
    
    public KCondition nilkew(
        final KColumn kColumn
    );
    
    public KCondition nilkew(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition nilkew(
        final KValTextField kValTextField
    );
    
    public KCondition nilkew(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition nilkew(
        final String value
    );
    
    public KCondition nilkew(
        final KOptionalString kOptionalString
    );
    
    public KCondition nilksw(
        final KColumn kColumn
    );
    
    public KCondition nilksw(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition nilksw(
        final KValTextField kValTextField
    );
    
    public KCondition nilksw(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition nilksw(
        final String value
    );
    
    public KCondition nilksw(
        final KOptionalString kOptionalString
    );
    
    public KCondition notIBetween(
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    );
    
    public KCondition notIBetween(
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    );
    
    public KCondition notIBetween(
        final String valueLow,
        final String valueHigh
    );
    
    public KCondition nibt(
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    );
    
    public KCondition nibt(
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    );
    
    public KCondition nibt(
        final String valueLow,
        final String valueHigh
    );
    
    public KCondition nieq(
        final KColumn kColumn
    );
    
    public KCondition nieq(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition nieq(
        final KValTextField kValTextField
    );
    
    public KCondition nieq(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition nieq(
        final String value
    );
    
    public KCondition nieq(
        final KOptionalString kOptionalString
    );
    
    public KCondition notIEqual(
        final KColumn kColumn
    );
    
    public KCondition notIEqual(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition notIEqual(
        final KValTextField kValTextField
    );
    
    public KCondition notIEqual(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition notIEqual(
        final String value
    );
    
    public KCondition notIEqual(
        final KOptionalString kOptionalString
    );
    
    public KCondition nigt(
        final KColumn kColumn
    );
    
    public KCondition nigt(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition nigt(
        final KValTextField kValTextField
    );
    
    public KCondition nigt(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition nigt(
        final String string
    );
    
    public KCondition nigt(
        final KOptionalString kOptionalString
    );
    
    public KCondition nigte(
        final KColumn kColumn
    );
    
    public KCondition nigte(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition nigte(
        final KValTextField kValTextField
    );
    
    public KCondition nigte(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition nigte(
        final String value
    );
    
    public KCondition nigte(
        final KOptionalString kOptionalString
    );
    
    public KCondition ilt(
        final KColumn kColumn
    );
    
    public KCondition ilt(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition ilt(
        final KValTextField kValTextField
    );
    
    public KCondition ilt(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition ilt(
        final String value
    );
    
    public KCondition ilt(
        final KOptionalString kOptionalString
    );
    
    public KCondition ilte(
        final KColumn kColumn
    );
    
    public KCondition ilte(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition ilte(
        final KValTextField kValTextField
    );
    
    public KCondition ilte(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition ilte(
        final String value
    );
    
    public KCondition ilte(
        final KOptionalString kOptionalString
    );
    
    public KCondition iLessThan(
        final KColumn kColumn
    );
    
    public KCondition iLessThan(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition iLessThan(
        final KValTextField kValTextField
    );
    
    public KCondition iLessThan(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition iLessThan(
        final String value
    );
    
    public KCondition iLessThan(
        final KOptionalString kOptionalString
    );
    
    public KCondition iLessThanOrEqualTo(
        final KColumn kColumn
    );
    
    public KCondition iLessThanOrEqualTo(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition iLessThanOrEqualTo(
        final KValTextField kValTextField
    );
    
    public KCondition iLessThanOrEqualTo(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition iLessThanOrEqualTo(
        final String value
    );
    
    public KCondition iLessThanOrEqualTo(
        final KOptionalString kOptionalString
    );
    
    public KCondition iLike(
        final KColumn kColumn
    );
    
    public KCondition iLike(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition iLike(
        final KValTextField kValTextField
    );
    
    public KCondition iLike(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition iLike(
        final String value
    );
    
    public KCondition iLike(
        final KOptionalString kOptionalString
    );
    
    public KCondition iLikeAny(
        final KColumn kColumn
    );
    
    public KCondition iLikeAny(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition iLikeAny(
        final KValTextField kValTextField
    );
    
    public KCondition iLikeAny(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition iLikeAny(
        final String value
    );
    
    public KCondition iLikeAny(
        final KOptionalString kOptionalString
    );
    
    public KCondition iLikeEndWith(
        final KColumn kColumn
    );
    
    public KCondition iLikeEndWith(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition iLikeEndWith(
        final KValTextField kValTextField
    );
    
    public KCondition iLikeEndWith(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition iLikeEndWith(
        final String value
    );
    
    public KCondition iLikeEndWith(
        final KOptionalString kOptionalString
    );
    
    public KCondition iLikeStartWith(
        final KColumn kColumn
    );
    
    public KCondition iLikeStartWith(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition iLikeStartWith(
        final KValTextField kValTextField
    );
    
    public KCondition iLikeStartWith(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition iLikeStartWith(
        final String value
    );
    
    public KCondition iLikeStartWith(
        final KOptionalString kOptionalString
    );
    
    public KCondition ilk(
        final KColumn kColumn
    );
    
    public KCondition ilk(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition ilk(
        final KValTextField kValTextField
    );
    
    public KCondition ilk(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition ilk(
        final String value
    );
    
    public KCondition ilk(
        final KOptionalString kOptionalString
    );
    
    public KCondition ilka(
        final KColumn kColumn
    );
    
    public KCondition ilka(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition ilka(
        final KValTextField kValTextField
    );
    
    public KCondition ilka(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition ilka(
        final String value
    );
    
    public KCondition ilka(
        final KOptionalString kOptionalString
    );
    
    public KCondition ilkew(
        final KColumn kColumn
    );
    
    public KCondition ilkew(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition ilkew(
        final KValTextField kValTextField
    );
    
    public KCondition ilkew(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition ilkew(
        final String value
    );
    
    public KCondition ilkew(
        final KOptionalString kOptionalString
    );
    
    public KCondition ilksw(
        final KColumn kColumn
    );
    
    public KCondition ilksw(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition ilksw(
        final KValTextField kValTextField
    );
    
    public KCondition ilksw(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition ilksw(
        final String value
    );
    
    public KCondition ilksw(
        final KOptionalString kOptionalString
    );
    
    public KCondition like(
        final KColumn kColumn
    );
    
    public KCondition like(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition like(
        final KValTextField kValTextField
    );
    
    public KCondition like(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition like(
        final String value
    );
    
    public KCondition like(
        final KOptionalString kOptionalString
    );
    
    public KCondition likeAny(
        final KColumn kColumn
    );
    
    public KCondition likeAny(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition likeAny(
        final KValTextField kValTextField
    );
    
     public KCondition likeAny(
        final KOptionalKValTextField kOptionalKValTextField
    );
     
    public KCondition likeAny(
        final String value
    );
    
    public KCondition likeAny(
        final KOptionalString kOptionalString
    );
    
    public KCondition likeEndWith(
        final KColumn kColumn
    );
    
    public KCondition likeEndWith(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition likeEndWith(
        final KValTextField kValTextField
    );
    
    public KCondition likeEndWith(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition likeEndWith(
        final String value
    );
    
    public KCondition likeEndWith(
        final KOptionalString kOptionalString
    );
    
    public KCondition likeStartWith(
        final KColumn kColumn
    );
    
    public KCondition likeStartWith(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition likeStartWith(
        final KValTextField kValTextField
    );
    
     public KCondition likeStartWith(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition likeStartWith(
        final String value
    );
    
    public KCondition likeStartWith(
        final KOptionalString kOptionalString
    );
    
    public KCondition lk(
        final KColumn kColumn
    );
    
    public KCondition lk(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition lk(
        final KValTextField kValTextField
    );
    
    public KCondition lk(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition lk(
        final String value
    );
    
    public KCondition lk(
        final KOptionalString kOptionalString
    );
    
    public KCondition lka(
        final KColumn kColumn
    );
    
    public KCondition lka(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition lka(
        final KValTextField kValTextField
    );
    
    public KCondition lka(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition lka(
        final String value
    );
    
    public KCondition lka(
        final KOptionalString kOptionalString
    );
    
    public KCondition lkew(
        final KColumn kColumn
    );
    
    public KCondition lkew(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition lkew(
        final KValTextField kValTextField
    );
    
    public KCondition lkew(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition lkew(
        final String value
    );
    
    public KCondition lkew(
        final KOptionalString kOptionalString
    );
    
    public KCondition lksw(
        final KColumn kColumn
    );
    
    public KCondition lksw(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition lksw(
        final KValTextField kValTextField
    );
    
    public KCondition lksw(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition lksw(
        final String value
    );
    
    public KCondition lksw(
        final KOptionalString kOptionalString
    );
    
    public KCondition nilt(
        final KColumn kColumn
    );
    
    public KCondition nilt(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition nilt(
        final KValTextField kValTextField
    );
    
    public KCondition nilt(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition nilt(
        final String value
    );
    
    public KCondition nilt(
        final KOptionalString kOptionalString
    );
    
    public KCondition nilte(
        final KColumn kColumn
    );
    
    public KCondition nilte(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition nilte(
        final KValTextField kValTextField
    );
    
    public KCondition nilte(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition nilte(
        final String value
    );
    
    public KCondition nilte(
        final KOptionalString kOptionalString
    );
    
    public KCondition notIGreaterThan(
        final KColumn kColumn
    );
    
    public KCondition notIGreaterThan(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition notIGreaterThan(
        final KValTextField kValTextField
    );
    
    public KCondition notIGreaterThan(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition notIGreaterThan(
        final String string
    );
    
    public KCondition notIGreaterThan(
        final KOptionalString kOptionalString
    );
    
    public KCondition notIGreaterThanOrEqualTo(
        final KColumn kColumn
    );
    
    public KCondition notIGreaterThanOrEqualTo(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition notIGreaterThanOrEqualTo(
        final KValTextField kValTextField
    );
    
    public KCondition notIGreaterThanOrEqualTo(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition notIGreaterThanOrEqualTo(
        final String value
    );
    
    public KCondition notIGreaterThanOrEqualTo(
        final KOptionalString kOptionalString
    );
    
    public KCondition notILessThan(
        final KColumn kColumn
    );
    
    public KCondition notILessThan(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition notILessThan(
        final KValTextField kValTextField
    );
    
    public KCondition notILessThan(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition notILessThan(
        final String value
    );
    
    public KCondition notILessThan(
        final KOptionalString kOptionalString
    );
    
    public KCondition notILessThanOrEqualTo(
        final KColumn kColumn
    );
    
    public KCondition notILessThanOrEqualTo(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition notILessThanOrEqualTo(
        final KValTextField kValTextField
    );
    
    public KCondition notILessThanOrEqualTo(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition notILessThanOrEqualTo(
        final String value
    );
    
    public KCondition notILessThanOrEqualTo(
        final KOptionalString kOptionalString
    );
    
    public KCondition notILike(
        final KColumn kColumn
    );
    
     public KCondition notILike(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition notILike(
        final KValTextField kValTextField
    );
    
    public KCondition notILike(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition notILike(
        final String value
    );
    
    public KCondition notILike(
        final KOptionalString kOptionalString
    );
    
    public KCondition notILikeAny(
        final KColumn kColumn
    );
    
    public KCondition notILikeAny(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition notILikeAny(
        final KValTextField kValTextField
    );
    
    public KCondition notILikeAny(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition notILikeAny(
        final String value
    );
    
    public KCondition notILikeAny(
        final KOptionalString kOptionalString
    );
    
    public KCondition notILikeEndWith(
        final KColumn kColumn
    );
    
    public KCondition notILikeEndWith(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition notILikeEndWith(
        final KValTextField kValTextField
    );
    
    public KCondition notILikeEndWith(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition notILikeEndWith(
        final String value
    );
    
    public KCondition notILikeEndWith(
        final KOptionalString kOptionalString
    );
    
    public KCondition notILikeStartWith(
        final KColumn kColumn
    );
    
    public KCondition notILikeStartWith(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition notILikeStartWith(
        final KValTextField kValTextField
    );
    
    public KCondition notILikeStartWith(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition notILikeStartWith(
        final String value
    );
    
    public KCondition notILikeStartWith(
        final KOptionalString kOptionalString
    );
    
    public KCondition nlk(
        final KColumn kColumn
    );
    
    public KCondition nlk(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition nlk(
        final KValTextField kValTextField
    );
    
    public KCondition nlk(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition nlk(
        final String value
    );
    
    public KCondition nlk(
        final KOptionalString kOptionalString
    );
    
    public KCondition nlka(
        final KColumn kColumn
    );
    
    public KCondition nlka(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition nlka(
        final KValTextField kValTextField
    );
    
    public KCondition nlka(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition nlka(
        final String value
    );
    
    public KCondition nlka(
        final KOptionalString kOptionalString
    );
    
    public KCondition nlkew(
        final KColumn kColumn
    );
    
    public KCondition nlkew(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition nlkew(
        final KValTextField kValTextField
    );
    
    public KCondition nlkew(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition nlkew(
        final String value
    );
    
    public KCondition nlkew(
        final KOptionalString kOptionalString
    );
    
    public KCondition nlksw(
        final KColumn kColumn
    );
    
    public KCondition nlksw(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition nlksw(
        final KValTextField kValTextField
    );
    
    public KCondition nlksw(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition nlksw(
        final String value
    );
    
    public KCondition nlksw(
        final KOptionalString kOptionalString
    );
    
    public KCondition notLike(
        final KColumn kColumn
    );
    
    public KCondition notLike(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition notLike(
        final KValTextField kValTextField
    );
    
    public KCondition notLike(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition notLike(
        final String value
    );
    
    public KCondition notLike(
        final KOptionalString kOptionalString
    );
    
    public KCondition notLikeAny(
        final KColumn kColumn
    );
    
    public KCondition notLikeAny(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition notLikeAny(
        final KValTextField kValTextField
    );
    
    public KCondition notLikeAny(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition notLikeAny(
        final String value
    );
    
    public KCondition notLikeAny(
        final KOptionalString kOptionalString
    );
    
    public KCondition notLikeEndWith(
        final KColumn kColumn
    );
    
    public KCondition notLikeEndWith(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition notLikeEndWith(
        final KValTextField kValTextField
    );
    
    public KCondition notLikeEndWith(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition notLikeEndWith(
        final String value
    );
    
    public KCondition notLikeEndWith(
        final KOptionalString kOptionalString
    );
    
    public KCondition notLikeStartWith(
        final KColumn kColumn
    );
    
    public KCondition notLikeStartWith(
        final KOptionalKColumn kOptionalKColumn
    );
    
    public KCondition notLikeStartWith(
        final KValTextField kValTextField
    );
    
    public KCondition notLikeStartWith(
        final KOptionalKValTextField kOptionalKValTextField
    );
    
    public KCondition notLikeStartWith(
        final String value
    );
    
    public KCondition notLikeStartWith(
        final KOptionalString kOptionalString
    );
}
