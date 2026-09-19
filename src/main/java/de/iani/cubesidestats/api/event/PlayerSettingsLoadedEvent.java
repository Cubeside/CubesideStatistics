package de.iani.cubesidestats.api.event;

import org.bukkit.entity.Player;
import org.bukkit.event.HandlerList;
import org.bukkit.event.player.PlayerEvent;

/**
 * Called after a player's settings have been loaded.
 *
 * @deprecated Player settings are guaranteed to be loaded when {@link org.bukkit.event.player.PlayerJoinEvent}
 *             is called. Read them directly from that event instead.
 */
@Deprecated
public class PlayerSettingsLoadedEvent extends PlayerEvent {
    private static final HandlerList handlers = new HandlerList();

    /**
     * @deprecated Player settings are guaranteed to be loaded by {@link org.bukkit.event.player.PlayerJoinEvent}.
     */
    @Deprecated
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
