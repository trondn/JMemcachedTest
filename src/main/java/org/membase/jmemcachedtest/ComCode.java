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

/**
 * @author Trond Norbye
 */
public enum ComCode {
    GET(0x00), SET(0x01), ADD(0x02), REPLACE(0x03), DELETE(0x04), INCREMENT(
            0x05), DECREMENT(0x06), QUIT(0x07), FLUSH(0x08), GETQ(0x09), NOOP(
            0x0a), VERSION(0x0b), GETK(0x0c), GETKQ(0x0d), APPEND(0x0e), PREPEND(
            0x0f), STAT(0x10), SETQ(0x11), ADDQ(0x12), REPLACEQ(0x13), DELETEQ(
            0x14), INCREMENTQ(0x15), DECREMENTQ(0x16), QUITQ(0x17), FLUSHQ(0x18), APPENDQ(
            0x19), PREPENDQ(0x1a), VERBOSITY(0x1b), SASL_LIST_MECHS(0x20),
        SASL_AUTH(0x21), SASL_STEP(0x22), RGET(0x30), RSET(0x31), RSETQ(0x32),
        RAPPEND(0x33), RAPPENDQ(0x34), RPREPEND(0x35), RPREPENDQ(0x36),
        RDELETE(0x37), RDELETEQ (0x38), RINCR(0x39), RINCRQ(0x3a), RDECR(0x3b),
        RDECRQ(0x3c), SET_VBUCKET(0x3d), GET_VBUCKET(0x3e), DEL_VBUCKET(0x3f),
        TAP_CONNECT(0x40), TAP_MUTATION(0x41), TAP_DELETE(0x42), TAP_FLUSH(0x43),
        TAP_OPAQUE(0x44), TAP_VBUCKET_SET(0x45), LAST_RESERVED(0xef),
        SCRUB(0xf0), ILLEGAL(0xff);

    private final byte value;

    ComCode(int value) {
        this.value = (byte) value;
    }

    byte cc() {
        return value;
    }

    static ComCode valueOf(byte cc) {
        switch (cc) {
        case 0x00:
            return GET;
        case 0x01:
            return SET;
        case 0x02:
            return ADD;
        case 0x03:
            return REPLACE;
        case 0x04:
            return DELETE;
        case 0x05:
            return INCREMENT;
        case 0x06:
            return DECREMENT;
        case 0x07:
            return QUIT;
        case 0x08:
            return FLUSH;
        case 0x09:
            return GETQ;
        case 0x0a:
            return NOOP;
        case 0x0b:
            return VERSION;
        case 0x0c:
            return GETK;
        case 0x0d:
            return GETKQ;
        case 0x0e:
            return APPEND;
        case 0x0f:
            return PREPEND;
        case 0x10:
            return STAT;
        case 0x11:
            return SETQ;
        case 0x12:
            return ADDQ;
        case 0x13:
            return REPLACEQ;
        case 0x14:
            return DELETEQ;
        case 0x15:
            return INCREMENTQ;
        case 0x16:
            return DECREMENTQ;
        case 0x17:
            return QUITQ;
        case 0x18:
            return FLUSHQ;
        case 0x19:
            return APPENDQ;
        case 0x1a:
            return PREPENDQ;
        default:
            return ILLEGAL;
        }
    }

    static String toString(ComCode cc) {
        switch (cc) {
        case SET:
            return "set";
        case ADD:
            return "add";
        case REPLACE:
            return "replace";
        case FLUSH:
            return "flush";
        case DELETE:
            return "delete";
        case INCREMENT:
            return "increment";
        case DECREMENT:
            return "decrement";
        case PREPEND:
            return "prepend";
        case APPEND:
            return "append";
        case VERSION:
            return "version";
        case NOOP:
            return "noop";
        case QUIT:
            return "quit";
        case GET:
            return "get";
        case GETQ:
            return "getq";
        case GETK:
            return "getk";
        case GETKQ:
            return "getkq";
        case ILLEGAL:
            return "Illegal";
        case STAT:
            return "stat";
        case SETQ:
            return "setq";
        case ADDQ:
            return "addq";
        case REPLACEQ:
            return "replaceq";
        case DELETEQ:
            return "deleteq";
        case INCREMENTQ:
            return "incrementq";
        case DECREMENTQ:
            return "decrementq";
        case QUITQ:
            return "quitq";
        case FLUSHQ:
            return "flushq";
        case APPENDQ:
            return "appendq";
        case PREPENDQ:
            return "prependq";

        default:
            return "unknown";

        }
    }
}