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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is similar to the RandomBucketWorker, except that it use the
 * REST API to create and delete the bucket. This means that it will utilize
 * the full stack of your membase server to create the buckets.
 *
 * You're going to completely lock up the admin server if you try to run
 * the commands as fast as possible, so do _not_ remove the sleeps in here ;)
 *
 * @author Trond Norbye
 */
public class RestBucketWorker implements Runnable {
    public static final String BUCKET_PASSWORD = "password";
    private final String BINARY;
    private final String HOST;
    private final int MAX_BUCKETS;

    RestBucketWorker(String host, String client, int max) {
        HOST = host;
        BINARY = client;
        MAX_BUCKETS = max;
    }

    public ProcessBuilder create(String bucket) {
        List<String> cmd = new ArrayList<String>();

        cmd.add(BINARY);
        cmd.add("bucket-create");
        cmd.add("-c");
        cmd.add(HOST);
        cmd.add("--bucket-type=membase");
        cmd.add("--bucket-password=" + BUCKET_PASSWORD);
        cmd.add("--bucket-ramsize=100");
        cmd.add("--bucket=" + bucket);
        cmd.add("--user=Administrator");
        cmd.add("--password=password");
        cmd.add("--bucket-replica=0");

        return new ProcessBuilder(cmd);
    }

    public ProcessBuilder delete(String bucket) {
        List<String> cmd = new ArrayList<String>();

        cmd.add(BINARY);
        cmd.add("bucket-delete");
        cmd.add("-c");
        cmd.add(HOST);
        cmd.add("--bucket=" + bucket);
        cmd.add("--user=Administrator");
        cmd.add("--password=password");

        return new ProcessBuilder(cmd);
    }

    public ProcessBuilder list() {
        List<String> cmd = new ArrayList<String>();

        cmd.add(BINARY);
        cmd.add("bucket-list");
        cmd.add("-c");
        cmd.add(HOST);
        cmd.add("--user=Administrator");
        cmd.add("--password=password");

        return new ProcessBuilder(cmd);
    }

    @Override
    public void run() {
        Random rand = new Random();

        while (true) {
            int next = rand.nextInt(30);
            if (next < 15) {
                executeCommand(create("" + rand.nextInt(MAX_BUCKETS)));
            } else if (next < 20) {
                executeCommand(delete("" + rand.nextInt(MAX_BUCKETS)));
            } else {
                executeCommand(list());
            }
        }
    }

    private void executeCommand(ProcessBuilder builder) {
        try {
            builder.redirectErrorStream();
            final Process process = builder.start();

            Thread t = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                        String s;
                        while ((s = reader.readLine()) != null) {
                            // ditch the output..
                        }

                        reader.close();
                    } catch (IOException ex) {
                        Logger.getLogger(RestBucketWorker.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            });
            t.start();
            try {
                t.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(RestBucketWorker.class.getName()).log(Level.SEVERE, null, ex);
            }

            try {
                process.waitFor();
            } catch (InterruptedException ex) {
                Logger.getLogger(RestBucketWorker.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(1);
            }

            try {
                process.getErrorStream().close();
            } catch (IOException e) {
            }
            try {
                process.getInputStream().close();
            } catch (IOException e) {
            }
            try {
                process.getOutputStream().close();
            } catch (IOException e) {
            }

        } catch (IOException ex) {
            Logger.getLogger(RestBucketWorker.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException ex) {
            Logger.getLogger(RestBucketWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
