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
import com.google.common.collect.Sets;
import net.william278.velocitab.config.Group;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Thread-safe registry for tracking remote players from other proxies.
 * Provides methods to add, remove, update, and query remote players.
 */
public class RemotePlayerRegistry {

    private final ConcurrentMap<UUID, RemoteTabPlayer> remotePlayers;
    private final ConcurrentMap<String, Set<UUID>> playersByProxy;

    public RemotePlayerRegistry() {
        this.remotePlayers = Maps.newConcurrentMap();
        this.playersByProxy = Maps.newConcurrentMap();
    }

    /**
     * Add a remote player to the registry.
     *
     * @param player The remote player to add
     */
    public void addRemotePlayer(@NotNull RemoteTabPlayer player) {
        remotePlayers.put(player.getUniqueId(), player);
        playersByProxy.computeIfAbsent(player.getProxyId(), k -> Sets.newConcurrentHashSet())
                .add(player.getUniqueId());
    }

    /**
     * Remove a remote player from the registry.
     *
     * @param uuid The UUID of the player to remove
     */
    public void removeRemotePlayer(@NotNull UUID uuid) {
        final RemoteTabPlayer player = remotePlayers.remove(uuid);
        if (player != null) {
            final Set<UUID> proxyPlayers = playersByProxy.get(player.getProxyId());
            if (proxyPlayers != null) {
                proxyPlayers.remove(uuid);
                if (proxyPlayers.isEmpty()) {
                    playersByProxy.remove(player.getProxyId());
                }
            }
        }
    }

    /**
     * Update an existing remote player or add if not present.
     *
     * @param player The remote player to update
     */
    public void updateRemotePlayer(@NotNull RemoteTabPlayer player) {
        player.updateTimestamp();
        final RemoteTabPlayer existing = remotePlayers.get(player.getUniqueId());
        if (existing != null && !existing.getProxyId().equals(player.getProxyId())) {
            // Proxy changed, remove from old proxy tracking
            final Set<UUID> oldProxyPlayers = playersByProxy.get(existing.getProxyId());
            if (oldProxyPlayers != null) {
                oldProxyPlayers.remove(player.getUniqueId());
                if (oldProxyPlayers.isEmpty()) {
                    playersByProxy.remove(existing.getProxyId());
                }
            }
        }
        addRemotePlayer(player);
    }

    /**
     * Get a remote player by UUID.
     *
     * @param uuid The UUID of the player
     * @return Optional containing the player if found
     */
    @NotNull
    public Optional<RemoteTabPlayer> getRemotePlayer(@NotNull UUID uuid) {
        return Optional.ofNullable(remotePlayers.get(uuid));
    }

    /**
     * Get all remote players.
     *
     * @return Collection of all remote players
     */
    @NotNull
    public Collection<RemoteTabPlayer> getAllRemotePlayers() {
        return new ArrayList<>(remotePlayers.values());
    }

    /**
     * Get all remote players on a specific server.
     *
     * @param serverName The server name
     * @return Collection of remote players on that server
     */
    @NotNull
    public Collection<RemoteTabPlayer> getRemotePlayersOnServer(@NotNull String serverName) {
        return remotePlayers.values().stream()
                .filter(player -> player.getServerName().equalsIgnoreCase(serverName))
                .collect(Collectors.toList());
    }

    /**
     * Get all remote players in a specific group.
     *
     * @param group The group to filter by
     * @return Collection of remote players in that group
     */
    @NotNull
    public Collection<RemoteTabPlayer> getRemotePlayersInGroup(@NotNull Group group) {
        return remotePlayers.values().stream()
                .filter(player -> player.getGroup().equals(group))
                .collect(Collectors.toList());
    }

    /**
     * Remove all players from a specific proxy.
     * Useful for cleaning up when a proxy goes offline.
     *
     * @param proxyId The proxy ID to purge
     */
    public void purgeProxy(@NotNull String proxyId) {
        final Set<UUID> proxyPlayers = playersByProxy.remove(proxyId);
        if (proxyPlayers != null) {
            proxyPlayers.forEach(remotePlayers::remove);
        }
    }

    /**
     * Clear all remote players from the registry.
     */
    public void clear() {
        remotePlayers.clear();
        playersByProxy.clear();
    }

    /**
     * Get the number of remote players registered.
     *
     * @return The count of remote players
     */
    public int size() {
        return remotePlayers.size();
    }
}
