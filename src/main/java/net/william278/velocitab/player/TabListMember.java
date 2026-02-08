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

package net.william278.velocitab.player;

import net.william278.velocitab.Velocitab;
import net.william278.velocitab.config.Group;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;
import java.util.UUID;

/**
 * Common interface for both local TabPlayer and remote players.
 * Provides access to essential player data needed for TAB list management.
 */
public interface TabListMember {

    /**
     * Get the unique ID of the player.
     *
     * @return The player's UUID
     */
    @NotNull
    UUID getUniqueId();

    /**
     * Get the username of the player.
     *
     * @return The player's username
     */
    @NotNull
    String getUsername();

    /**
     * Get the server name the player is currently on.
     *
     * @return The server name
     */
    @NotNull
    String getServerName();

    /**
     * Get the TAB group this player belongs to.
     *
     * @return The player's group
     */
    @NotNull
    Group getGroup();

    /**
     * Set the TAB group for this player.
     *
     * @param group The group to set
     */
    void setGroup(@NotNull Group group);

    /**
     * Get the team name for this player.
     *
     * @param plugin The plugin instance
     * @return The team name
     */
    @NotNull
    String getTeamName(@NotNull Velocitab plugin);

    /**
     * Check if the player is vanished.
     *
     * @return true if vanished, false otherwise
     */
    boolean isVanished();

    /**
     * Check if this is a remote player (on another proxy).
     *
     * @return true if remote, false if local
     */
    boolean isRemote();

    /**
     * Check if the player data is fully loaded.
     *
     * @return true if loaded, false otherwise
     */
    boolean isLoaded();

    /**
     * Get the player's ping latency in milliseconds.
     *
     * @return The latency value
     */
    int getLatency();

    /**
     * Get the custom name for this player, if set.
     *
     * @return Optional containing the custom name, or empty if not set
     */
    @NotNull
    Optional<String> getCustomName();
}
