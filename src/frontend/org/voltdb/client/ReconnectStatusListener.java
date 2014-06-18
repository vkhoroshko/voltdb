/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;

/**
 * This listener allows to reconnect to a single server with retry after connection loss by
 * running a daemon thread that retries connection until success with a simple limited exponential backoff.
 */
public class ReconnectStatusListener extends ClientStatusListenerExt implements ClientAwareListener {
    private static final VoltLogger LOG = new VoltLogger("HOST");

    static final int DEFAULT_INITIAL_RETRY_INTERVAL_MS = 1000;
    static final int DEFAULT_MAX_RETRY_INTERVAL_MS = 8000;

    private final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "Retry Connection");
            thread.setDaemon(true);
            return thread;
        }
    });

    private final int m_initialRetryIntervalMS;
    private final int m_maxRetryIntervalMS;
    private Client m_client;

    public ReconnectStatusListener() {
        this(DEFAULT_INITIAL_RETRY_INTERVAL_MS, DEFAULT_MAX_RETRY_INTERVAL_MS);
    }

    public ReconnectStatusListener(int initialRetryIntervalMS, int maxRetryIntervalMS) {
        if (maxRetryIntervalMS < initialRetryIntervalMS) {
            throw new IllegalArgumentException("max retry interval can't be less than initial retry interval");
        }
        this.m_initialRetryIntervalMS = initialRetryIntervalMS;
        this.m_maxRetryIntervalMS = maxRetryIntervalMS;
    }

    @Override
    public void connectionLost(final String hostname, final int port, int connectionsLeft, DisconnectCause cause) {
        LOG.warn(String.format("Connection to VoltDB node at: %s:%d was lost.", hostname, port));

        executor.execute(new Runnable() {
            @Override
            public void run() {
                connectToOneServerWithRetry(hostname, port);
            }
        });
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param hostname host name
     * @param port     port
     */
    private void connectToOneServerWithRetry(String hostname, int port) {
        int sleep = m_initialRetryIntervalMS;
        while (true) {
            try {
                m_client.createConnection(hostname, port);
                LOG.info(String.format("Connected to VoltDB node at %s:%d.", hostname, port));
                break;
            } catch (Exception e) {
                LOG.warn(String.format("Connection to VoltDB node at %s:%d failed - retrying in %d second(s).",
                        hostname, port, TimeUnit.MILLISECONDS.toSeconds(sleep)));
                try {
                    Thread.sleep(sleep);
                } catch (Exception ignored) {
                }
                if (sleep < m_maxRetryIntervalMS) {
                    sleep += sleep;
                }
            }
        }
    }

    @Override
    public void setClient(Client client) {
        this.m_client = client;
    }
}
