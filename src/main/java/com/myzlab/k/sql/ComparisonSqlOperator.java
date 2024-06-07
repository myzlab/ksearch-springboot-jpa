package com.myzlab.k.sql;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ComparisonSqlOperator {
    
    /* TRGM operators */
    public static final ComparisonSqlOperator TRGM_SIMILARITY = new ComparisonSqlOperator("%");
    public static final ComparisonSqlOperator TRGM_WORD_SIMILARITY = new ComparisonSqlOperator("<%");
    public static final ComparisonSqlOperator TRGM_WORD_SIMILARITY_COMMUTATOR = new ComparisonSqlOperator("%>");
    public static final ComparisonSqlOperator TRGM_STRICT_WORD_SIMILARITY = new ComparisonSqlOperator("<<%");
    public static final ComparisonSqlOperator TRGM_STRICT_WORD_SIMILARITY_COMMUTATOR = new ComparisonSqlOperator("%>>");
    public static final ComparisonSqlOperator TRGM_SIMILARITY_DISTANCE = new ComparisonSqlOperator("<->");
    public static final ComparisonSqlOperator TRGM_WORD_SIMILARITY_DISTANCE = new ComparisonSqlOperator("<<->");
    public static final ComparisonSqlOperator TRGM_WORD_SIMILARITY_DISTANCE_COMMUTATOR = new ComparisonSqlOperator("<->>");
    public static final ComparisonSqlOperator TRGM_STRICT_WORD_SIMILARITY_DISTANCE = new ComparisonSqlOperator("<<<->");
    public static final ComparisonSqlOperator TRGM_STRICT_WORD_SIMILARITY_DISTANCE_COMMUTATOR = new ComparisonSqlOperator("<->>>");
    
    /* Full-text search operator */
    public static final ComparisonSqlOperator TEXT_SEARCH_MATCH = new ComparisonSqlOperator("@@");

    private final String sql;
}