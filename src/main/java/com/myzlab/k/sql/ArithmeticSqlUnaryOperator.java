package com.myzlab.k.sql;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ArithmeticSqlUnaryOperator {
    
    public static final ArithmeticSqlUnaryOperator SQUARE_ROOT = new ArithmeticSqlUnaryOperator("|/", false);
    public static final ArithmeticSqlUnaryOperator CUBE_ROOT = new ArithmeticSqlUnaryOperator("||/", false);
    public static final ArithmeticSqlUnaryOperator FACTORIAL = new ArithmeticSqlUnaryOperator("!", true);
    public static final ArithmeticSqlUnaryOperator ABSOLUTE_VALUE = new ArithmeticSqlUnaryOperator("@", false);
    public static final ArithmeticSqlUnaryOperator BITWISE_NOT = new ArithmeticSqlUnaryOperator("~", false);
    
    private final String sql;
    private final boolean addToRightSide;
}