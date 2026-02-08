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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

public record MultiProxyMessage(
        @NotNull String sourceProxyId,
        @NotNull MultiProxyMessageType type,
        @Nullable UUID playerUuid,
        @Nullable String playerName,
        @Nullable String serverName,
        @Nullable String groupName,
        @Nullable String teamName,
        @Nullable String prefix,
        @Nullable String suffix,
        @Nullable String customName,
        boolean vanished,
        int latency,
        long timestamp
) {

    private static final Gson GSON = new GsonBuilder().create();

    @NotNull
    public String toJson() {
        return GSON.toJson(this);
    }

    @NotNull
    public static MultiProxyMessage fromJson(@NotNull String json) {
        return GSON.fromJson(json, MultiProxyMessage.class);
    }

}
