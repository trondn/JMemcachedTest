/*
 * CDDL HEADER START
 * 
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
 * 
 * See LICENSE.txt included in this distribution for the specific
 * language governing permissions and limitations under the License.
 * 
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at LICENSE.txt.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 * 
 * CDDL HEADER END
 */

/*
 * Copyright 2010 Trond Norbye.  All rights reserved.
 * Use is subject to license terms.
 */

package org.membase.jmemcachedtest;

import java.nio.ByteBuffer;

/**
 *
 * @author trond
 */
class BinarySaslAuthCommand extends BinaryCommand {

    public BinarySaslAuthCommand(int bucket) {
        String plain = "PLAIN";
        String username = "" + bucket;
        String password = "password";

        array = new byte[24 + 2 + plain.length() + username.length() + password.length()];
        ByteBuffer cmd = ByteBuffer.wrap(array);
        cmd.put((byte) 0x80); // magic
        cmd.put(ComCode.SASL_AUTH.cc()); // com code
        cmd.putShort((short) plain.length()); // keylen
        cmd.put((byte) 0); //extlen
        cmd.put((byte) 0); //datatype
        cmd.putShort((short) 0); // bucket
        cmd.putInt(2 + plain.length() + username.length() + password.length()); // bodylen
        cmd.putInt(0xdeadbeef); // opaque
        cmd.putLong(0); // cas
        cmd.put(plain.getBytes());
        cmd.put((byte)0);
        cmd.put(username.getBytes());
        cmd.put((byte)0);
        cmd.put(password.getBytes());
    }
}
