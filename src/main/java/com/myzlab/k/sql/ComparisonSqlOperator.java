package com.myzlab.k.sql;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ComparisonSqlOperator {
    
    TRGM_SIMILARITY("%"),
    TRGM_WORD_SIMILARITY("<%"),
    TRGM_WORD_SIMILARITY_COMMUTATOR("%>"),
    TRGM_STRICT_WORD_SIMILARITY("<<%"),
    TRGM_STRICT_WORD_SIMILARITY_COMMUTATOR("%>>"),
    TRGM_SIMILARITY_DISTANCE("<->"),
    TRGM_WORD_SIMILARITY_DISTANCE("<<->"),
    TRGM_WORD_SIMILARITY_DISTANCE_COMMUTATOR("<->>"),
    TRGM_STRICT_WORD_SIMILARITY_DISTANCE("<<<->"),
    TRGM_STRICT_WORD_SIMILARITY_DISTANCE_COMMUTATOR("<->>>");

    private final String sql;
}