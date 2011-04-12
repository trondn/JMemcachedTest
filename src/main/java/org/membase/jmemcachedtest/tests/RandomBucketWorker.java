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

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.membase.jmemcachedtest.MemcachedClient;

/**
 * This is a test program used to hammer a standalone memcached with bucket
 * engine. To run this program you should export the environment variable:
 * BUCKET_ENGINE_DIABLE_AUTH_PHASE before you start memcached and bucket engine.
 *
 * This test program connects to localhost on port 11211 and will nuke
 * the directories from the engine between each time it creates the bucket
 * to avoid reading the database the next time we generate the same bucket.
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class RandomBucketWorker implements Runnable {

    MemcachedClient client;
    // @todo fix for windows
    private static final String MODULE = "/opt/membase/bin/ep_engine/ep.so";
    private final String config;
    private final String bucketname;
    private final File dbdir;

    public RandomBucketWorker(String hostname, int port, String bucketname, File dataroot) {
        client = new MemcachedClient(hostname, port);
        StringBuilder sb = new StringBuilder();
        sb.append("vb0=false;");
        sb.append("waitforwarmup=false;");
        sb.append("ht_size=3079;");
        sb.append("ht_locks=5;");
        sb.append("failpartialwarmup=false;");
        sb.append("db_shards=4;");
        sb.append("shardpattern=%d/%b-%i.mb;");
        sb.append("db_strategy=multiMTVBDB;");
        sb.append("tap_noop_interval=20;");
        sb.append("max_txn_size=1000;");
        sb.append("max_size=104857600;");
        sb.append("tap_keepalive=300;");

        if (!dataroot.exists()) {
            dataroot.mkdirs();
        }
        dbdir = new File(dataroot, bucketname);
        purgeDbDir();
        File dbname = new File(dbdir, "db");
        sb.append("dbname=");
        sb.append(dbname.getAbsolutePath());
        sb.append(";");
        config = sb.toString();
        this.bucketname = bucketname;
    }

    @Override
    public void run() {
        try {
            while (true) {
                if (client.createBucket(bucketname, MODULE, config)) {
                    if (!client.selectBucket(bucketname)) {
                        throw new IOException("Failed to select bucket");                   
                    }
                    for (int ii = 0; ii < 1024; ++ii) {
                        client.setVbucket(ii);
                    }
                    client.bulkLoad(10, 100, 120, 150);
                    if (client.deleteBucket(bucketname, true)) {
                        purgeDbDir();
                    }
                } else {
                    System.out.println("Failed");

                }
            }
        } catch (IOException ex) {
            Logger.getLogger(RandomBucketWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void rmdir(File dir) {
        for (File f : dir.listFiles()) {
            if (f.isDirectory()) {
                if (f.getName().startsWith(".")) {
                    rmdir(f);
                }
            } else {
                f.delete();
            }
        }
        dir.delete();
    }

    private void purgeDbDir() {
        if (dbdir.exists()) {
            rmdir(dbdir);
        }
        dbdir.mkdir();
    }

    public static void main(String argv[]) {
        for (int ii = 0; ii < 10; ++ii) {
            Thread t = new Thread(new RandomBucketWorker("localhost", 11211, "foo-" + ii, new File("/tmp/bucket-tester")));
            t.start();
        }            
    }
}
