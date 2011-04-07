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

package org.membase.jmemcachedtest;

import java.nio.ByteBuffer;

/**
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class BinaryDeleteBucket extends BinaryCommand {
    static final String FORCE = "force=true";

    public BinaryDeleteBucket(String bucket, boolean force) {
        int length = bucket.length();
        if (force) {
            length += FORCE.length();
        }

        array = new byte[24 + length];
        ByteBuffer cmd = ByteBuffer.wrap(array);
        cmd.put((byte) 0x80); // magic
        cmd.put(ComCode.DELETE_BUCKET.cc()); // com code
        cmd.putShort((short) bucket.length()); // keylen
        cmd.put((byte) 0); //extlen
        cmd.put((byte) 0); //datatype
        cmd.putShort((short) 0); // bucket
        cmd.putInt(length); // bodylen
        cmd.putInt(0xdeadbeef); // opaque
        cmd.putLong(0); // cas
        cmd.put(bucket.getBytes());
        if (force) {
            cmd.put(FORCE.getBytes());
        }
    }
}
