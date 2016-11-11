/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author Sergey Sitnikov
 */
public class VarIntSerializerTest {

  // @formatter:off
  final Map<Integer, byte[]> UNSIGNED_INTEGER_TEST_VECTORS = new LinkedHashMap<Integer, byte[]>() {{
    put(0,                  bytes(0x00));
    put(1,                  bytes(0x01));
    put(10,                 bytes(0x0A));
    put(64,                 bytes(0x40));
    put(127,                bytes(0X7F));
    put(128,                bytes(0x80, 0x01));
    put(150,                bytes(0x96, 0x01));
    put(300,                bytes(0xAC, 0x02));
    put(11111,              bytes(0xE7, 0x56));
    put(16383,              bytes(0xFF, 0x7F));
    put(16384,              bytes(0x80, 0x80, 0x01));
    put(Integer.MAX_VALUE,  bytes(0xFF, 0xFF, 0xFF, 0xFF, 0x07));
  }};
  // @formatter:on

  @Test
  public void testUnsignedInteger() {
    for (Map.Entry<Integer, byte[]> vector : UNSIGNED_INTEGER_TEST_VECTORS.entrySet()) {
      final int expectedValue = vector.getKey();
      final byte[] expectedBytes = vector.getValue();

      final int actualLength = OVarIntSerializer.sizeOfUnsigned(expectedValue);
      final byte[] actualBytes = new byte[actualLength];
      final ByteBuffer actualBytesBuffer = ByteBuffer.wrap(actualBytes);
      OVarIntSerializer.writeUnsigned(expectedValue, actualBytesBuffer);
      assertArrayEquals(Integer.toString(expectedValue), expectedBytes, actualBytes);
      assertEquals(expectedBytes.length, actualBytesBuffer.position());

      actualBytesBuffer.position(0);
      final int actualValue = OVarIntSerializer.readUnsignedInteger(actualBytesBuffer);
      assertEquals(expectedValue, actualValue);
      assertEquals(expectedBytes.length, actualBytesBuffer.position());

      actualBytesBuffer.position(0);
      assertEquals(Integer.toString(expectedValue), expectedBytes.length,
          OVarIntSerializer.sizeOfSerializedValue(actualBytesBuffer));
      assertEquals(expectedBytes.length, actualBytesBuffer.position());
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testSizeOfInvalidInput() {
    OVarIntSerializer
        .sizeOfSerializedValue(ByteBuffer.wrap(bytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00)));
  }

  @Test(expected = IllegalStateException.class)
  public void testReadInvalidInput() {
    OVarIntSerializer.readUnsignedLong(ByteBuffer.wrap(bytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00)));
  }

  private static byte[] bytes(int... bytes) {
    final byte[] result = new byte[bytes.length];
    for (int i = 0; i < bytes.length; ++i)
      result[i] = (byte) bytes[i];
    return result;
  }
}
