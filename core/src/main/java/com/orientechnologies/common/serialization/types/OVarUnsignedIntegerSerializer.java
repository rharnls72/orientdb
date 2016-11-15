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

package com.orientechnologies.common.serialization.types;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.nio.ByteBuffer;

/**
 * @author Sergey Sitnikov
 */
public class OVarUnsignedIntegerSerializer implements OBinarySerializer<Integer> {

  /**
   * The identifier of the serializer.
   */
  public static final byte ID = 31;

  /**
   * The global stateless instance of the serializer.
   */
  public static final OVarUnsignedIntegerSerializer INSTANCE = new OVarUnsignedIntegerSerializer();

  @Override
  public int getObjectSize(Integer object, Object... hints) {
    return OVarIntSerializer.sizeOfUnsigned(object);
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return getObjectSizeInByteBuffer(ByteBuffer.wrap(stream, startPosition, stream.length - startPosition));
  }

  @Override
  public void serialize(Integer object, byte[] stream, int startPosition, Object... hints) {
    serializeInByteBufferObject(object, ByteBuffer.wrap(stream, startPosition, stream.length - startPosition), hints);
  }

  @Override
  public Integer deserialize(byte[] stream, int startPosition) {
    return deserializeFromByteBufferObject(ByteBuffer.wrap(stream, startPosition, stream.length - startPosition));
  }

  @Override
  public byte getId() {
    return ID;
  }

  @Override
  public boolean isFixedLength() {
    return false;
  }

  @Override
  public int getFixedLength() {
    throw new UnsupportedOperationException("OVarUnsignedIntegerSerializer is not a fixed-length serializer");
  }

  @Override
  public void serializeNativeObject(Integer object, byte[] stream, int startPosition, Object... hints) {
    serialize(object, stream, startPosition, hints);
  }

  @Override
  public Integer deserializeNativeObject(byte[] stream, int startPosition) {
    return deserialize(stream, startPosition);
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return getObjectSize(stream, startPosition);
  }

  @Override
  public Integer preprocess(Integer value, Object... hints) {
    return value;
  }

  @Override
  public void serializeInByteBufferObject(Integer object, ByteBuffer buffer, Object... hints) {
    OVarIntSerializer.writeUnsigned(object, buffer);
  }

  @Override
  public Integer deserializeFromByteBufferObject(ByteBuffer buffer) {
    return OVarIntSerializer.readUnsignedInteger(buffer);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return OVarIntSerializer.sizeOfSerializedValue(buffer);
  }

  @Override
  public Integer deserializeFromByteBufferObject(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return OVarIntSerializer.readUnsignedInteger(buffer, walChanges, offset);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return OVarIntSerializer.sizeOfSerializedValue(buffer, walChanges, offset);
  }
}
