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

import lombok.Getter;
import lombok.Setter;
import net.william278.velocitab.Velocitab;
import net.william278.velocitab.config.Group;
import net.william278.velocitab.player.TabListMember;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.UUID;

/**
 * Represents a player connected to another proxy in a multi-proxy setup.
 * Unlike TabPlayer, this does not hold a reference to a Velocity Player object
 * and stores pre-resolved placeholder values.
 */
@Getter
@Setter
public class RemoteTabPlayer implements TabListMember {

    private final UUID uuid;
    private final String username;
    private final String proxyId;
    private String serverName;
    @NotNull
    private Group group;
    @Nullable
    private String teamName;
    @Nullable
    private String prefix;
    @Nullable
    private String suffix;
    @Nullable
    private String customName;
    private boolean vanished;
    private int latency;
    private boolean loaded;
    private long lastUpdated;

    public RemoteTabPlayer(@NotNull UUID uuid, @NotNull String username, @NotNull String proxyId,
                          @NotNull String serverName, @NotNull Group group) {
        this.uuid = uuid;
        this.username = username;
        this.proxyId = proxyId;
        this.serverName = serverName;
        this.group = group;
        this.loaded = false;
        this.vanished = false;
        this.latency = 0;
        this.lastUpdated = System.currentTimeMillis();
    }

    @Override
    @NotNull
    public UUID getUniqueId() {
        return uuid;
    }

    @Override
    @NotNull
    public String getUsername() {
        return username;
    }

    @Override
    @NotNull
    public String getServerName() {
        return serverName;
    }

    @Override
    @NotNull
    public Group getGroup() {
        return group;
    }

    @Override
    public void setGroup(@NotNull Group group) {
        this.group = group;
    }

    @Override
    @NotNull
    public String getTeamName(@NotNull Velocitab plugin) {
        if (teamName == null) {
            // Remote players store team name explicitly; if not set, return empty string
            // In future phases, this will be properly resolved by the sorting manager
            return "";
        }
        return teamName;
    }

    @Override
    public boolean isVanished() {
        return vanished;
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public boolean isLoaded() {
        return loaded;
    }

    @Override
    public int getLatency() {
        return latency;
    }

    @Override
    @NotNull
    public Optional<String> getCustomName() {
        return Optional.ofNullable(customName);
    }

    /**
     * Update the last updated timestamp to the current time.
     */
    public void updateTimestamp() {
        this.lastUpdated = System.currentTimeMillis();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof RemoteTabPlayer other && uuid.equals(other.uuid);
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }
}
