package de.iani.cubesidestats.api;

public class PlayerWithScore {
    private final PlayerStatistics player;
    private final int score;
    private final int position;

    public PlayerWithScore(PlayerStatistics player, int score, int position) {
        this.player = player;
        this.score = score;
        this.position = position;
    }

    public PlayerStatistics getPlayer() {
        return player;
    }

    public int getScore() {
        return score;
    }

    public int getPosition() {
        return position;
    }
}
