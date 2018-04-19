package de.iani.cubesidestats.api;

public interface GamePlayerCount {
    public void addLocalPlayers(String game, int amount);

    public void subtractLocalPlayers(String game, int amount);

    public void setLocalPlayers(String game, int amount);

    public int getLocalPlayers(String game);

    public int getPlayers(String game);
}
