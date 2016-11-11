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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALPageChangesPortion;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * @author Sergey Sitnikov
 */
public class VarLinkSerializerTest {

  private static final OVarLinkSerializer SERIALIZER   = OVarLinkSerializer.INSTANCE;
  private static final ORecordId          RID          = new ORecordId(1, 128);
  private static final byte[]             BYTES        = bytes(0x01, 0x80, 0x01);
  private static final byte[]             EXTRA_BYTES  = bytes(0xFF, 0x01, 0x80, 0x01, 0xFF);
  private static final ByteBuffer         BUFFER       = ByteBuffer.wrap(BYTES);
  private static final ByteBuffer         EXTRA_BUFFER = ByteBuffer.wrap(EXTRA_BYTES);

  private static final ORecordId   CHANGED_RID       = new ORecordId(5, 150);
  private static final byte[]      PAGE              = new byte[1024];
  private static final byte[]      EXTRA_PAGE        = new byte[1024];
  private static final ByteBuffer  PAGE_BUFFER       = ByteBuffer.wrap(PAGE);
  private static final ByteBuffer  EXTRA_PAGE_BUFFER = ByteBuffer.wrap(EXTRA_PAGE);
  private static final OWALChanges CHANGES           = new OWALPageChangesPortion(1024);
  private static final OWALChanges EXTRA_CHANGES     = new OWALPageChangesPortion(1024);

  static {
    System.arraycopy(BYTES, 0, PAGE, 0, BYTES.length);
    CHANGES.setByteValue(PAGE_BUFFER, (byte) 5, 0);
    CHANGES.setByteValue(PAGE_BUFFER, (byte) 0x96, 1);

    System.arraycopy(EXTRA_BYTES, 0, EXTRA_PAGE, 0, EXTRA_BYTES.length);
    EXTRA_CHANGES.setByteValue(EXTRA_PAGE_BUFFER, (byte) 5, 1);
    EXTRA_CHANGES.setByteValue(EXTRA_PAGE_BUFFER, (byte) 0x96, 2);
  }

  @Test
  public void testRids() {
    assertEquals(BYTES.length, SERIALIZER.getObjectSize(RID));
  }

  @Test
  public void testArrays() {
    assertEquals(BYTES.length, SERIALIZER.getObjectSize(BYTES, 0));
    assertEquals(BYTES.length, SERIALIZER.getObjectSize(EXTRA_BYTES, 1));

    assertEquals(RID, SERIALIZER.deserialize(BYTES, 0));
    assertEquals(RID, SERIALIZER.deserialize(EXTRA_BYTES, 1));

    byte[] actualBytes = new byte[3];
    SERIALIZER.serialize(RID, actualBytes, 0);
    Assert.assertArrayEquals(BYTES, actualBytes);

    byte[] actualExtraBytes = bytes(0xFF, 0x00, 0x00, 0x00, 0xFF);
    SERIALIZER.serialize(RID, actualExtraBytes, 1);
    Assert.assertArrayEquals(EXTRA_BYTES, actualExtraBytes);

    assertEquals(RID, SERIALIZER.deserializeNativeObject(BYTES, 0));
    assertEquals(RID, SERIALIZER.deserializeNativeObject(EXTRA_BYTES, 1));

    assertEquals(BYTES.length, SERIALIZER.getObjectSizeNative(BYTES, 0));
    assertEquals(BYTES.length, SERIALIZER.getObjectSizeNative(EXTRA_BYTES, 1));

    actualBytes = new byte[3];
    SERIALIZER.serializeNativeObject(RID, actualBytes, 0);
    Assert.assertArrayEquals(BYTES, actualBytes);

    actualExtraBytes = bytes(0xFF, 0x00, 0x00, 0x00, 0xFF);
    SERIALIZER.serializeNativeObject(RID, actualExtraBytes, 1);
    Assert.assertArrayEquals(EXTRA_BYTES, actualExtraBytes);
  }

  @Test
  public void testBuffers() {
    BUFFER.position(0);
    assertEquals(BYTES.length, SERIALIZER.getObjectSizeInByteBuffer(BUFFER));

    BUFFER.position(0);
    assertEquals(RID, SERIALIZER.deserializeFromByteBufferObject(BUFFER));

    final byte[] actualBytes = new byte[3];
    SERIALIZER.serializeInByteBufferObject(RID, ByteBuffer.wrap(actualBytes));
    Assert.assertArrayEquals(BYTES, actualBytes);
  }

  @Test
  public void testWalChanges() {
    assertEquals(BYTES.length, SERIALIZER.getObjectSizeInByteBuffer(PAGE_BUFFER, CHANGES, 0));
    assertEquals(BYTES.length, SERIALIZER.getObjectSizeInByteBuffer(EXTRA_PAGE_BUFFER, EXTRA_CHANGES, 1));

    assertEquals(CHANGED_RID, SERIALIZER.deserializeFromByteBufferObject(PAGE_BUFFER, CHANGES, 0));
    assertEquals(CHANGED_RID, SERIALIZER.deserializeFromByteBufferObject(EXTRA_PAGE_BUFFER, EXTRA_CHANGES, 1));
  }

  private static byte[] bytes(int... bytes) {
    final byte[] result = new byte[bytes.length];
    for (int i = 0; i < bytes.length; ++i)
      result[i] = (byte) bytes[i];
    return result;
  }
}
