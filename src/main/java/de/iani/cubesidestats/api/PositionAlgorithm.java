package de.iani.cubesidestats.api;

/**
 * Determines how the position is calculated if there are multiple entries with the same score
 */
public enum PositionAlgorithm {
    /**
     * If the first and the second entries have the same score: 1,2,3,4,...
     */
    TOTAL_ORDER,
    /**
     * If the first and the second entries have the same score: 1,1,3,4,...
     */
    SKIP_POSITIONS_AFTER_DUPLICATES,
    /**
     * If the first and the second entries have the same score: 1,1,2,3,...
     */
    DO_NOT_SKIP_POSITIONS_AFTER_DUPLICATES
}
