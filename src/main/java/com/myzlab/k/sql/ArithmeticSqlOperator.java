package com.myzlab.k.sql;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ArithmeticSqlOperator {
    
    ADD("+"), 
    SUBTRACT("-"), 
    MULTIPLY("*"), 
    DIVIDE("/"),
    MODULO("%");
    
    private final String sql;
}