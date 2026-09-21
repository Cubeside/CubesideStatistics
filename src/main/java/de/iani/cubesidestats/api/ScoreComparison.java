package de.iani.cubesidestats.api;

/** Comparison operators for indexed score searches. */
public enum ScoreComparison {
    EQUAL("="),
    NOT_EQUAL("<>"),
    GREATER_THAN(">"),
    GREATER_OR_EQUAL(">="),
    LESS_THAN("<"),
    LESS_OR_EQUAL("<=");

    private final String sqlOperator;

    ScoreComparison(String sqlOperator) {
        this.sqlOperator = sqlOperator;
    }

    public String getSqlOperator() {
        return sqlOperator;
    }
}
