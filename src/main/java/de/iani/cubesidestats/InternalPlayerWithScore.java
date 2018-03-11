package de.iani.cubesidestats;

import java.util.UUID;

public class InternalPlayerWithScore {
    private final UUID player;
    private final int score;
    private final int position;

    public InternalPlayerWithScore(UUID player, int score, int position) {
        this.player = player;
        this.score = score;
        this.position = position;
    }

    public UUID getPlayer() {
        return player;
    }

    public int getScore() {
        return score;
    }

    public int getPosition() {
        return position;
    }
}
