/**
 *     Copyright 2011 Membase, Inc.
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
package org.membase.jmemcachedtest;

import java.io.IOException;
import java.net.Socket;
import java.util.Random;

/**
 * This memcached driver is only used to drive get-set load to the memcached
 * server so that I may look at it's performance!
 * 
 * @author Trond Norbye
 */
public class MemcachedClient {

    private Socket socket;
    private BinaryProtocolPipe pipe;
    private final String host;
    private final int port;

    public MemcachedClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private void ensurePipe() throws IOException {
        if (pipe == null) {
            socket = new Socket(host, port);
            pipe = new BinaryProtocolPipe(socket);
        }
    }

    public int get(int key) throws IOException {
        ensurePipe();
        pipe.send(new BinaryGetCommand(key));
        BinaryResponse rsp = pipe.nextResponse();
        if (rsp.getStatus() != ErrorCode.SUCCESS) {
            return -1;
        } else {
            return rsp.getDataSize();
        }
    }

    public boolean store(int key, int datasz) throws IOException {
        ensurePipe();
        pipe.send(new BinarySetCommand(key, datasz));
        BinaryResponse rsp = pipe.nextResponse();
        return rsp.getStatus() == ErrorCode.SUCCESS;
    }

    public boolean setVbucket(int vbucket) throws IOException {
        ensurePipe();
        pipe.send(new BinaryCreateVBucketCommand(vbucket));
        BinaryResponse rsp = pipe.nextResponse();
        return rsp.getStatus() == ErrorCode.SUCCESS;
    }

    public void shutdown() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ex) {
            // DO nothing
        }
    }

    public void bulkLoad(int start, int stop, int minsz, int maxsz) throws IOException {
        Random random = new Random();
        BinaryNoopCommand noop = new BinaryNoopCommand();
        maxsz -= minsz;
        ensurePipe();
        for (int ii = 0; ii < stop; ++ii) {
            pipe.send(new BinarySetQCommand(ii, random.nextInt(maxsz) + minsz), false);
            if ((ii % 100) == 0) {
                // send noop and verify that I receive a noop
                pipe.send(noop);
                BinaryResponse rsp = pipe.nextResponse();
                if (rsp.getCode() != ComCode.NOOP) {
                    throw new IOException("Failed to store data!");
                }
            }
        }
        // send noop and verify that I receive a noop
        pipe.send(noop);
        BinaryResponse rsp = pipe.nextResponse();
        if (rsp.getCode() != ComCode.NOOP) {
            throw new IOException("Failed to store data!");
        }
    }
}
