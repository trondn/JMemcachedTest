/*
 *     Copyright 2011 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.membase.jmemcachedtest.tests;

import java.io.IOException;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.membase.jmemcachedtest.MemcachedClient;

/**
 * This is a small dummy client that just drives load to a bucket.
 *
 * @author Trond Norbye
 */
public class LoadClient implements Runnable {

    private final String host;
    private final int port;
    private final String auth;
    private final String password;
    private static final int MEMCACHED_OPS_SLEEPTIME = 250;
    private static final int MEMCACHED_RECONNECT_SLEEPTIME = 1000;

    public LoadClient(String host, int port, String auth, String password) {
        this.host = host;
        this.port = port;
        this.auth = auth;
        this.password = password;
    }

    private void sleep(int amount) {
        try {
            Thread.sleep(amount);
        } catch (InterruptedException ex) {
            Logger.getLogger(LoadClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void run() {
        Random rand = new Random();
        while (true) {
            MemcachedClient client = new MemcachedClient(host, port, auth, password);

            try {
                while (true) {
                    if (rand.nextInt(30) < 20) {
                        client.get(rand.nextInt(1000));
                    } else {
                        int a = rand.nextInt(1000) + 2;
                        client.bulkLoad(a / 2, a, 120, 150);
                    }
                    rand.nextInt(MEMCACHED_OPS_SLEEPTIME);
                }
            } catch (IOException ex) {
                sleep(MEMCACHED_RECONNECT_SLEEPTIME);
            }
        }
    }
}
