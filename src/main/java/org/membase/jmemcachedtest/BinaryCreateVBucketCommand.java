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

import java.nio.ByteBuffer;

/**
 *
 * @author Trond Norbye
 */
public class BinaryCreateVBucketCommand extends BinaryCommand {

    public BinaryCreateVBucketCommand(int vbucket) {
        array = new byte[28];
        ByteBuffer cmd = ByteBuffer.wrap(array);
        cmd.put((byte) 0x80); // magic
        cmd.put(ComCode.SET_VBUCKET.cc()); // com code
        cmd.putShort((short)0); // keylen
        cmd.put((byte) 0); //extlen
        cmd.put((byte) 0); //datatype
        cmd.putShort((short)vbucket); // bucket
        cmd.putInt(4); // bodylen
        cmd.putInt(0xdeadbeef); // opaque
        cmd.putLong(0); // cas
        cmd.putInt(1); // active
    }
}
