package de.iani.cubesidestats.api.event;

import org.bukkit.entity.Player;
import org.bukkit.event.HandlerList;
import org.bukkit.event.player.PlayerEvent;

public class PlayerSettingsLoadedEvent extends PlayerEvent {
    private static final HandlerList handlers = new HandlerList();

    public PlayerSettingsLoadedEvent(final Player who) {
        super(who);
    }

    @Override
    public HandlerList getHandlers() {
        return handlers;
    }

    public static HandlerList getHandlerList() {
        return handlers;
    }
}
