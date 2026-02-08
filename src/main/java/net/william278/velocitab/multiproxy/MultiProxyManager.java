/*
 * This file is part of Velocitab, licensed under the Apache License 2.0.
 *
 *  Copyright (c) William278 <will27528@gmail.com>
 *  Copyright (c) contributors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.william278.velocitab.multiproxy;

import com.google.common.collect.Maps;
import net.william278.velocitab.Velocitab;
import net.william278.velocitab.config.Group;
import net.william278.velocitab.player.TabPlayer;
import net.william278.velocitab.tab.Nametag;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrates multi-proxy event broadcasting and handling.
 * Subscribes to Redis channels on startup, handles incoming messages,
 * and provides methods for broadcasting local events.
 */
public class MultiProxyManager {

    private static final String CHANNEL_PLAYER_JOIN = "velocitab:player_join";
    private static final String CHANNEL_PLAYER_LEAVE = "velocitab:player_leave";
    private static final String CHANNEL_PLAYER_SWITCH = "velocitab:player_switch";
    private static final String CHANNEL_PLAYER_UPDATE = "velocitab:player_update";
    private static final String CHANNEL_HEARTBEAT = "velocitab:heartbeat";

    private static final long HEARTBEAT_INTERVAL_SECONDS = 15;
    private static final long PROXY_TIMEOUT_MS = 60_000; // 60 seconds

    private final Velocitab plugin;
    private final MultiProxyBroker broker;
    private final RemotePlayerRegistry registry;
    private final Logger logger;
    private final Map<String, Long> proxyHeartbeats;
    private final Map<UUID, Long> messageTimestamps;
    private final Map<UUID, String> lastBroadcastState;

    public MultiProxyManager(@NotNull Velocitab plugin,
                             @NotNull MultiProxyBroker broker,
                             @NotNull RemotePlayerRegistry registry) {
        this.plugin = plugin;
        this.broker = broker;
        this.registry = registry;
        this.logger = plugin.getLogger();
        this.proxyHeartbeats = Maps.newConcurrentMap();
        this.messageTimestamps = Maps.newConcurrentMap();
        this.lastBroadcastState = Maps.newConcurrentMap();
    }

    /**
     * Initialize the manager by subscribing to all Redis channels
     * and starting the heartbeat mechanism.
     */
    public void initialize() {
        subscribeToChannels();
        startHeartbeat();
        startPlaceholderRefreshTask();
        logger.info("Multi-proxy manager initialized");
    }

    /**
     * Shutdown the manager by disconnecting the broker.
     */
    public void shutdown() {
        broker.disconnect();
        logger.info("Multi-proxy manager shut down");
    }

    /**
     * Subscribe to all Redis channels.
     */
    private void subscribeToChannels() {
        broker.subscribe(CHANNEL_PLAYER_JOIN, json -> {
            plugin.getServer().getScheduler()
                    .buildTask(plugin, () -> handleRemoteJoin(MultiProxyMessage.fromJson(json)))
                    .schedule();
        });

        broker.subscribe(CHANNEL_PLAYER_LEAVE, json -> {
            plugin.getServer().getScheduler()
                    .buildTask(plugin, () -> handleRemoteLeave(MultiProxyMessage.fromJson(json)))
                    .schedule();
        });

        broker.subscribe(CHANNEL_PLAYER_SWITCH, json -> {
            plugin.getServer().getScheduler()
                    .buildTask(plugin, () -> handleRemoteSwitch(MultiProxyMessage.fromJson(json)))
                    .schedule();
        });

        broker.subscribe(CHANNEL_PLAYER_UPDATE, json -> {
            plugin.getServer().getScheduler()
                    .buildTask(plugin, () -> handleRemoteUpdate(MultiProxyMessage.fromJson(json)))
                    .schedule();
        });

        broker.subscribe(CHANNEL_HEARTBEAT, json -> {
            plugin.getServer().getScheduler()
                    .buildTask(plugin, () -> handleHeartbeat(MultiProxyMessage.fromJson(json)))
                    .schedule();
        });
    }

    /**
     * Start the heartbeat mechanism that periodically sends heartbeat messages
     * and checks for stale proxies.
     */
    private void startHeartbeat() {
        plugin.getServer().getScheduler()
                .buildTask(plugin, () -> {
                    sendHeartbeat();
                    checkStaleProxies();
                })
                .repeat(HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS)
                .schedule();
    }

    /**
     * Start the placeholder refresh task that periodically re-broadcasts
     * placeholder data for all local players to ensure consistency.
     */
    private void startPlaceholderRefreshTask() {
        final int refreshInterval = plugin.getSettings().getMultiProxyPlaceholderRefreshInterval();
        if (refreshInterval <= 0) {
            logger.info("Placeholder refresh task disabled (interval set to 0 or less)");
            return;
        }

        plugin.getServer().getScheduler()
                .buildTask(plugin, this::refreshAllLocalPlayers)
                .repeat(refreshInterval, TimeUnit.SECONDS)
                .schedule();
        logger.info("Placeholder refresh task scheduled to run every {} seconds", refreshInterval);
    }

    /**
     * Refresh placeholder data for all local players by broadcasting
     * updates only if their state has changed since the last broadcast.
     */
    private void refreshAllLocalPlayers() {
        int broadcastCount = 0;
        for (TabPlayer player : plugin.getTabList().getPlayers().values()) {
            if (broadcastUpdateIfChanged(player)) {
                broadcastCount++;
            }
        }
        if (broadcastCount > 0) {
            logger.debug("Refreshed {} local players with changed state", broadcastCount);
        }
    }

    /**
     * Send a heartbeat message to other proxies.
     */
    public void sendHeartbeat() {
        final MultiProxyMessage message = new MultiProxyMessage(
                broker.getProxyId(),
                MultiProxyMessageType.HEARTBEAT,
                null, null, null, null, null, null, null, null,
                false, 0, System.currentTimeMillis()
        );
        publishAsync(CHANNEL_HEARTBEAT, message);
    }

    /**
     * Check for stale proxies that haven't sent a heartbeat recently
     * and purge their players from the registry.
     * Also cleans up old message timestamps to prevent memory growth.
     * <p>
     * This method runs every 15 seconds (heartbeat interval) to:
     * - Remove proxies that haven't sent a heartbeat in 60 seconds
     * - Remove message timestamps older than 5 minutes to prevent unbounded growth
     */
    private void checkStaleProxies() {
        final long now = System.currentTimeMillis();
        
        // Check for stale proxies (no heartbeat in 60 seconds)
        proxyHeartbeats.entrySet().removeIf(entry -> {
            final String proxyId = entry.getKey();
            final long lastHeartbeat = entry.getValue();
            if (now - lastHeartbeat > PROXY_TIMEOUT_MS) {
                logger.warn("Proxy {} timed out, purging remote players", proxyId);
                registry.purgeProxy(proxyId);
                return true;
            }
            return false;
        });
        
        // Clean up message timestamps older than 5 minutes to prevent memory growth
        messageTimestamps.entrySet().removeIf(entry -> {
            final long timestamp = entry.getValue();
            return now - timestamp > 300_000; // 5 minutes
        });
    }

    /**
     * Broadcast a player join event to other proxies.
     *
     * @param player The player who joined
     */
    public void broadcastJoin(@NotNull TabPlayer player) {
        final MultiProxyMessage message = buildPlayerMessage(
                player, MultiProxyMessageType.PLAYER_JOIN
        );
        publishAsync(CHANNEL_PLAYER_JOIN, message);
        
        // Track initial state
        lastBroadcastState.put(player.getUniqueId(), buildStateHash(player));
    }

    /**
     * Broadcast a player leave event to other proxies.
     *
     * @param player The player who left
     */
    public void broadcastLeave(@NotNull TabPlayer player) {
        final MultiProxyMessage message = new MultiProxyMessage(
                broker.getProxyId(),
                MultiProxyMessageType.PLAYER_LEAVE,
                player.getUniqueId(),
                player.getUsername(),
                null, null, null, null, null, null,
                false, 0, System.currentTimeMillis()
        );
        publishAsync(CHANNEL_PLAYER_LEAVE, message);
        
        // Clean up state tracking
        lastBroadcastState.remove(player.getUniqueId());
    }

    /**
     * Broadcast a server switch event to other proxies.
     *
     * @param player The player who switched servers
     */
    public void broadcastSwitch(@NotNull TabPlayer player) {
        final MultiProxyMessage message = buildPlayerMessage(
                player, MultiProxyMessageType.PLAYER_SWITCH
        );
        publishAsync(CHANNEL_PLAYER_SWITCH, message);
    }

    /**
     * Broadcast a player update event to other proxies.
     * This is used for vanish toggles, custom name changes, etc.
     *
     * @param player The player whose data was updated
     */
    public void broadcastUpdate(@NotNull TabPlayer player) {
        final MultiProxyMessage message = buildPlayerMessage(
                player, MultiProxyMessageType.PLAYER_UPDATE
        );
        publishAsync(CHANNEL_PLAYER_UPDATE, message);
        
        // Update last broadcast state
        lastBroadcastState.put(player.getUniqueId(), buildStateHash(player));
    }

    /**
     * Broadcast a player update event only if the player's state has changed
     * since the last broadcast. Used for periodic refresh optimization.
     *
     * @param player The player to check and potentially broadcast
     * @return true if an update was broadcast, false otherwise
     */
    private boolean broadcastUpdateIfChanged(@NotNull TabPlayer player) {
        final String currentState = buildStateHash(player);
        final String previousState = lastBroadcastState.get(player.getUniqueId());
        
        if (!currentState.equals(previousState)) {
            broadcastUpdate(player);
            return true;
        }
        return false;
    }

    /**
     * Build a hash string representing the current state of a player's
     * placeholder values and vanish state for change detection.
     *
     * @param player The player to hash
     * @return A string representing the player's current state
     */
    @NotNull
    private String buildStateHash(@NotNull TabPlayer player) {
        final Nametag nametag = player.getNametag(plugin);
        final String teamName = player.getTeamName(plugin);
        final String customName = player.getCustomName().orElse("");
        
        return String.format("%s|%s|%s|%s|%b|%d",
                teamName != null ? teamName : "",
                nametag.prefix() != null ? nametag.prefix() : "",
                nametag.suffix() != null ? nametag.suffix() : "",
                customName,
                player.isVanished(),
                player.getLatency()
        );
    }

    /**
     * Build a complete player message with all necessary data.
     */
    @NotNull
    private MultiProxyMessage buildPlayerMessage(@NotNull TabPlayer player,
                                                  @NotNull MultiProxyMessageType type) {
        final Nametag nametag = player.getNametag(plugin);
        final String teamName = player.getTeamName(plugin);

        return new MultiProxyMessage(
                broker.getProxyId(),
                type,
                player.getUniqueId(),
                player.getUsername(),
                player.getServerName(),
                player.getGroup().name(),
                teamName,
                nametag.prefix(),
                nametag.suffix(),
                player.getCustomName().orElse(null),
                player.isVanished(),
                player.getLatency(),
                System.currentTimeMillis()
        );
    }

    /**
     * Publish a message asynchronously to avoid blocking the calling thread.
     */
    private void publishAsync(@NotNull String channel, @NotNull MultiProxyMessage message) {
        plugin.getServer().getScheduler()
                .buildTask(plugin, () -> {
                    try {
                        broker.publish(channel, message.toJson());
                    } catch (Exception e) {
                        logger.error("Failed to publish message to channel {}", channel, e);
                    }
                })
                .schedule();
    }

    /**
     * Handle a remote player join message.
     */
    private void handleRemoteJoin(@NotNull MultiProxyMessage message) {
        // Ignore our own broadcasts
        if (message.sourceProxyId().equals(broker.getProxyId())) {
            return;
        }

        // Check for stale messages
        if (!isMessageCurrent(message)) {
            return;
        }

        // Check for null group name
        if (message.groupName() == null) {
            logger.warn("Received remote join with null group name for player: {}", message.playerName());
            return;
        }

        // Resolve the group
        final Optional<Group> groupOpt = plugin.getTabGroupsManager().getGroup(message.groupName());

        if (groupOpt.isEmpty()) {
            logger.warn("Received remote join for unknown group: {}", message.groupName());
            return;
        }

        final Group group = groupOpt.get();
        final RemoteTabPlayer remotePlayer = new RemoteTabPlayer(
                message.playerUuid(),
                message.playerName(),
                message.sourceProxyId(),
                message.serverName(),
                group
        );

        // Set additional properties
        remotePlayer.setTeamName(message.teamName());
        remotePlayer.setPrefix(message.prefix());
        remotePlayer.setSuffix(message.suffix());
        remotePlayer.setCustomName(message.customName());
        remotePlayer.setVanished(message.vanished());
        remotePlayer.setLatency(message.latency());
        remotePlayer.setLoaded(true);

        // Add to registry
        registry.addRemotePlayer(remotePlayer);

        // Add to local players' TAB lists
        plugin.getTabList().addRemoteEntry(remotePlayer);

        // Create scoreboard team for the remote player
        plugin.getScoreboardManager().createRemoteTeam(remotePlayer);

        logger.debug("Remote player joined: {} from proxy {}", message.playerName(), message.sourceProxyId());
    }

    /**
     * Handle a remote player leave message.
     */
    private void handleRemoteLeave(@NotNull MultiProxyMessage message) {
        // Ignore our own broadcasts
        if (message.sourceProxyId().equals(broker.getProxyId())) {
            return;
        }

        // Get the remote player before removing
        final Optional<RemoteTabPlayer> remoteOpt = registry.getRemotePlayer(message.playerUuid());

        // Remove from registry
        registry.removeRemotePlayer(message.playerUuid());

        // Remove from local players' TAB lists
        plugin.getTabList().removeRemoteEntry(message.playerUuid());

        // Remove scoreboard team for the remote player
        remoteOpt.ifPresent(remote -> plugin.getScoreboardManager().removeRemoteTeam(remote));

        logger.debug("Remote player left: {} from proxy {}", message.playerName(), message.sourceProxyId());
    }

    /**
     * Handle a remote player server switch message.
     */
    private void handleRemoteSwitch(@NotNull MultiProxyMessage message) {
        // Ignore our own broadcasts
        if (message.sourceProxyId().equals(broker.getProxyId())) {
            return;
        }

        // Check for stale messages
        if (!isMessageCurrent(message)) {
            return;
        }

        // Get existing remote player
        final Optional<RemoteTabPlayer> existingOpt = registry.getRemotePlayer(message.playerUuid());
        if (existingOpt.isEmpty()) {
            logger.debug("Received switch for unknown player {}, treating as join", message.playerName());
            handleRemoteJoin(message);
            return;
        }

        // Check for null group name
        if (message.groupName() == null) {
            logger.warn("Received remote switch with null group name for player: {}", message.playerName());
            return;
        }

        // Resolve the new group
        final Optional<Group> groupOpt = plugin.getTabGroupsManager().getGroup(message.groupName());

        if (groupOpt.isEmpty()) {
            logger.warn("Received remote switch for unknown group: {}", message.groupName());
            return;
        }

        final Group newGroup = groupOpt.get();
        final RemoteTabPlayer remotePlayer = existingOpt.get();

        // Store old team name to check if it changed
        final String oldTeamName = remotePlayer.getTeamName(plugin);

        // Update the player's properties
        remotePlayer.setServerName(message.serverName());
        remotePlayer.setGroup(newGroup);
        remotePlayer.setTeamName(message.teamName());
        remotePlayer.setPrefix(message.prefix());
        remotePlayer.setSuffix(message.suffix());
        remotePlayer.setCustomName(message.customName());
        remotePlayer.setVanished(message.vanished());
        remotePlayer.setLatency(message.latency());
        remotePlayer.updateTimestamp();

        // Update in registry
        registry.updateRemotePlayer(remotePlayer);

        // Remove and re-add to re-evaluate group visibility
        plugin.getTabList().removeRemoteEntry(message.playerUuid());
        plugin.getTabList().addRemoteEntry(remotePlayer);

        // Update scoreboard team if team name changed
        final String newTeamName = remotePlayer.getTeamName(plugin);
        if (!Objects.equals(oldTeamName, newTeamName)) {
            plugin.getScoreboardManager().updateRemoteTeam(remotePlayer);
        }

        logger.debug("Remote player switched: {} to server {}", message.playerName(), message.serverName());
    }

    /**
     * Handle a remote player update message (vanish, custom name, etc.).
     */
    private void handleRemoteUpdate(@NotNull MultiProxyMessage message) {
        // Ignore our own broadcasts
        if (message.sourceProxyId().equals(broker.getProxyId())) {
            return;
        }

        // Check for stale messages
        if (!isMessageCurrent(message)) {
            return;
        }

        // Get existing remote player
        final Optional<RemoteTabPlayer> existingOpt = registry.getRemotePlayer(message.playerUuid());
        if (existingOpt.isEmpty()) {
            logger.debug("Received update for unknown player {}, treating as join", message.playerName());
            handleRemoteJoin(message);
            return;
        }

        final RemoteTabPlayer remotePlayer = existingOpt.get();
        final boolean wasVanished = remotePlayer.isVanished();
        final String oldTeamName = remotePlayer.getTeamName(plugin);

        // Update the player's properties (including vanish state)
        remotePlayer.setTeamName(message.teamName());
        remotePlayer.setPrefix(message.prefix());
        remotePlayer.setSuffix(message.suffix());
        remotePlayer.setCustomName(message.customName());
        remotePlayer.setVanished(message.vanished());
        remotePlayer.setLatency(message.latency());
        remotePlayer.updateTimestamp();

        // Update in registry
        registry.updateRemotePlayer(remotePlayer);

        // If vanish state changed, handle visibility
        // Note: vanish state is already updated above, so team operations see the new state
        if (wasVanished != message.vanished()) {
            if (message.vanished()) {
                // Player is now vanished - remove from TAB and remove team
                plugin.getTabList().removeRemoteEntry(message.playerUuid());
                plugin.getScoreboardManager().removeRemoteTeam(remotePlayer);
            } else {
                // Player is now visible - add to TAB and create team
                plugin.getTabList().addRemoteEntry(remotePlayer);
                plugin.getScoreboardManager().createRemoteTeam(remotePlayer);
            }
        } else {
            // Otherwise just update the entry
            plugin.getTabList().updateRemoteEntry(remotePlayer);

            // Update team if team name or nametag changed
            final String newTeamName = remotePlayer.getTeamName(plugin);
            if (!Objects.equals(oldTeamName, newTeamName)) {
                plugin.getScoreboardManager().updateRemoteTeam(remotePlayer);
            }
        }

        logger.debug("Remote player updated: {}", message.playerName());
    }

    /**
     * Handle a heartbeat message from another proxy.
     */
    private void handleHeartbeat(@NotNull MultiProxyMessage message) {
        // Ignore our own heartbeats
        if (message.sourceProxyId().equals(broker.getProxyId())) {
            return;
        }

        proxyHeartbeats.put(message.sourceProxyId(), message.timestamp());
        logger.debug("Received heartbeat from proxy {}", message.sourceProxyId());
    }

    /**
     * Check if a message is current based on timestamp.
     * Discards stale messages if a newer message for the same player was already processed.
     */
    private boolean isMessageCurrent(@NotNull MultiProxyMessage message) {
        if (message.playerUuid() == null) {
            return true;
        }

        final Long lastTimestamp = messageTimestamps.get(message.playerUuid());
        if (lastTimestamp != null && message.timestamp() <= lastTimestamp) {
            logger.debug("Discarding stale message for player {}", message.playerName());
            return false;
        }

        messageTimestamps.put(message.playerUuid(), message.timestamp());
        return true;
    }

    /**
     * Broadcast all local players as join messages.
     * Used during plugin reload to re-sync state.
     */
    public void broadcastAllLocalPlayers() {
        plugin.getTabList().getPlayers().values().forEach(this::broadcastJoin);
        logger.info("Broadcasted {} local players", plugin.getTabList().getPlayers().size());
    }
}
