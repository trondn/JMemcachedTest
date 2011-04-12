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

/**
 * "kill" membase by driving a hard bucket creation/deletion/list load while
 * we're running traffic to the cluster as well :)
 *
 * @author Trond Norbye
 */
public class MembaseKiller {

    public static final int MAX_BUCKETS = 10;
    public static final int NUM_BUCKET_THREADS = 3;

    public static final String RestServer = "localhost:8091";
    public static final String Moxi = "localhost";
    public static final int MoxiPort = 11211;


    public static void main(String[] args) {
        for (int ii = 0; ii < NUM_BUCKET_THREADS; ++ii) {
            Thread t = new Thread(new RestBucketWorker(RestServer, "/opt/membase/bin/cli/membase", MAX_BUCKETS));
            t.start();
        }

        for (int ii = 0; ii < MAX_BUCKETS; ++ii) {
            for (int jj = 0; jj < 5; ++jj) {
                Thread t = new Thread(new LoadClient(Moxi, MoxiPort, "" + ii, RestBucketWorker.BUCKET_PASSWORD));
                t.start();
            }
        }
    }

    private MembaseKiller() {
    }
}
