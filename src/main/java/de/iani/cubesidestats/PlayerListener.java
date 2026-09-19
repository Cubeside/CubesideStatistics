package de.iani.cubesidestats;

import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import net.kyori.adventure.text.Component;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

public class PlayerListener implements Listener {

    private static final Component SETTINGS_LOAD_FAILED_MESSAGE = Component.text("Deine Spielerdaten konnten nicht geladen werden. Bitte versuche es später erneut.");

    private final CubesideStatisticsImplementation stats;
    private final ConcurrentHashMap<AsyncPlayerPreLoginEvent, UUID> preloadedProfiles = new ConcurrentHashMap<>();

    public PlayerListener(CubesideStatisticsImplementation stats) {
        this.stats = stats;
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onPreLogin(AsyncPlayerPreLoginEvent event) {
        preloadedProfiles.remove(event);
        if (event.getLoginResult() != AsyncPlayerPreLoginEvent.Result.ALLOWED) {
            return;
        }

        UUID playerId = getCurrentPlayerId(event);
        if (loadSettings(event, playerId)) {
            preloadedProfiles.put(event, playerId);
        }
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void verifyPreLoginProfile(AsyncPlayerPreLoginEvent event) {
        UUID preloadedPlayerId = preloadedProfiles.remove(event);
        if (event.getLoginResult() != AsyncPlayerPreLoginEvent.Result.ALLOWED) {
            return;
        }

        UUID currentPlayerId = getCurrentPlayerId(event);
        if (!currentPlayerId.equals(preloadedPlayerId)) {
            loadSettings(event, currentPlayerId);
        }
    }

    private UUID getCurrentPlayerId(AsyncPlayerPreLoginEvent event) {
        UUID profileId = event.getPlayerProfile().getId();
        return profileId == null ? event.getUniqueId() : profileId;
    }

    private boolean loadSettings(AsyncPlayerPreLoginEvent event, UUID playerId) {
        try {
            stats.preparePlayerSettings(playerId).join();
            return true;
        } catch (CompletionException | CancellationException e) {
            Throwable cause = e;
            while (cause instanceof CompletionException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            stats.getPlugin().getLogger().log(Level.SEVERE, "Could not load settings for " + playerId, cause);
            event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER, SETTINGS_LOAD_FAILED_MESSAGE);
            return false;
        }
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onJoin(PlayerJoinEvent event) {
        Player player = event.getPlayer();
        stats.playerJoined(player);
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onQuit(PlayerQuitEvent event) {
        Player player = event.getPlayer();
        stats.playerDisconnected(player);
    }
}
