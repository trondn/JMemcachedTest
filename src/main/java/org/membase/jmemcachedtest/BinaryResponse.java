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
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 *
 * @author Trond Norbye
 */
class BinaryResponse {

    ByteBuffer header;

    public BinaryResponse(InputStream is) throws IOException {
        byte h[] = new byte[24];
        header = ByteBuffer.wrap(h);

        if (is.read(h) != h.length) {
            throw new IOException("Illegal read!");
        }

        int body = header.getInt(8);
        if (body > 0) {
            byte data[] = new byte[header.getInt(8)];
            int nr = is.read(data);
            if (nr != data.length) {
                throw new IOException("Incomplete read!");
            }
        }
    }

    ErrorCode getStatus() {
        return ErrorCode.valueOf(header.getShort(6));
    }

    int getDataSize() {
        return header.getInt(8) - header.getShort(2) - header.get(4);
    }

    ComCode getCode() {
        return ComCode.valueOf(header.get(1));
    }
}
