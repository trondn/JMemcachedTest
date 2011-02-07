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
 * @author Trond Norbye
 */
public class BinaryGetCommand extends BinaryCommand {
    public BinaryGetCommand(int key) {
        String s = Integer.toString(key);
        array = new byte[24 + s.length()];

        ByteBuffer cmd = ByteBuffer.wrap(array);
        cmd.put((byte)0x80); // magic
        cmd.put(ComCode.GET.cc()); // com code
        cmd.putShort((short) s.length()); // keylen
        cmd.put((byte)0); //extlen
        cmd.put((byte)0); //datatype
        cmd.putShort((short) (key % 1024)); // bucket
        cmd.putInt(array.length - 24); // bodylen
        cmd.putInt(0xdeadbeef); // opaque
        cmd.putLong(0); // cas
        cmd.put(s.getBytes()); // add the key
    }
}
