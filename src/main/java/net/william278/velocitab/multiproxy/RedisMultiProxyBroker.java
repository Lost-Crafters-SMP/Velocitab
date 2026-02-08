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

import net.william278.velocitab.config.RedisSettings;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class RedisMultiProxyBroker implements MultiProxyBroker {

    private final String proxyId;
    private final RedisSettings settings;
    private final Logger logger;
    private JedisPool jedisPool;
    private Thread subscriberThread;
    private final Map<String, Consumer<String>> handlers = new ConcurrentHashMap<>();
    private volatile boolean running = false;

    public RedisMultiProxyBroker(@NotNull String proxyId, @NotNull RedisSettings settings, @NotNull Logger logger) {
        this.proxyId = proxyId;
        this.settings = settings;
        this.logger = logger;
    }

    @Override
    public void connect() {
        try {
            final JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(8);
            poolConfig.setMaxIdle(8);
            poolConfig.setMinIdle(1);

            if (settings.password().isEmpty()) {
                jedisPool = new JedisPool(poolConfig, settings.host(), settings.port(), 2000, settings.useSsl());
            } else {
                jedisPool = new JedisPool(poolConfig, settings.host(), settings.port(), 2000, settings.password(), settings.useSsl());
            }

            // Test connection
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.ping();
            }

            running = true;
            startSubscriberThread();
            logger.info("Connected to Redis at {}:{}", settings.host(), settings.port());
        } catch (Exception e) {
            logger.error("Failed to connect to Redis", e);
            throw new RuntimeException("Failed to connect to Redis", e);
        }
    }

    @Override
    public void disconnect() {
        running = false;
        if (subscriberThread != null && subscriberThread.isAlive()) {
            subscriberThread.interrupt();
        }
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
            logger.info("Disconnected from Redis");
        }
    }

    @Override
    public void publish(@NotNull String channel, @NotNull String message) {
        if (jedisPool == null || jedisPool.isClosed()) {
            logger.warn("Cannot publish message: Redis connection not available");
            return;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.publish(channel, message);
        } catch (Exception e) {
            logger.error("Failed to publish message to channel: " + channel, e);
        }
    }

    @Override
    public void subscribe(@NotNull String channel, @NotNull Consumer<String> handler) {
        handlers.put(channel, handler);
        logger.debug("Subscribed to channel: {}", channel);
        
        // Start subscriber thread if not already running and we're connected
        if (running && (subscriberThread == null || !subscriberThread.isAlive())) {
            startSubscriberThread();
        }
    }

    @Override
    @NotNull
    public String getProxyId() {
        return proxyId;
    }

    private void startSubscriberThread() {
        if (handlers.isEmpty()) {
            return;
        }

        subscriberThread = new Thread(() -> {
            while (running) {
                try (Jedis jedis = jedisPool.getResource()) {
                    final String[] channels = handlers.keySet().toArray(new String[0]);
                    jedis.subscribe(new JedisPubSub() {
                        @Override
                        public void onMessage(String channel, String message) {
                            final Consumer<String> handler = handlers.get(channel);
                            if (handler != null) {
                                try {
                                    handler.accept(message);
                                } catch (Exception e) {
                                    logger.error("Error handling message from channel: " + channel, e);
                                }
                            }
                        }
                    }, channels);
                } catch (Exception e) {
                    if (running) {
                        logger.error("Redis subscriber thread error, reconnecting in 5 seconds...", e);
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
        }, "Velocitab-Redis-Subscriber");
        subscriberThread.setDaemon(true);
        subscriberThread.start();
    }

}
