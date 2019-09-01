package de.iani.cubesidestats.api;

import com.google.common.base.Preconditions;

public final class PlayerAchivementQueryKey implements StatisticsQueryKey {
    private final PlayerStatistics player;
    private final AchivementKey key;

    public PlayerAchivementQueryKey(PlayerStatistics player, AchivementKey key) {
        Preconditions.checkNotNull(player, "player");
        Preconditions.checkNotNull(key, "key");
        this.player = player;
        this.key = key;
    }

    public PlayerStatistics getPlayer() {
        return player;
    }

    public AchivementKey getKey() {
        return key;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass() != PlayerAchivementQueryKey.class) {
            return false;
        }
        PlayerAchivementQueryKey other = (PlayerAchivementQueryKey) obj;
        return player.equals(other.player) && key == other.key;
    }

    @Override
    public int hashCode() {
        return player.hashCode() * 23 + key.hashCode();
    }
}
