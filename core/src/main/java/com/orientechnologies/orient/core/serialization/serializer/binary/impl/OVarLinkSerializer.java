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

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.nio.ByteBuffer;

/**
 * The variable-size version of {@link OLinkSerializer}, uses var-int encoding to encode the cluster and the cluster position.
 */
public class OVarLinkSerializer implements OBinarySerializer<OIdentifiable> {

  /**
   * The identifier of the serializer.
   */
  public static final byte ID = 33;

  /**
   * The global stateless instance of the serializer.
   */
  public static final OVarLinkSerializer INSTANCE = new OVarLinkSerializer();

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
    throw new UnsupportedOperationException("OVarLinkSerializer is not a fixed-length serializer");
  }

  @Override
  public OIdentifiable preprocess(OIdentifiable value, Object... hints) {
    return value == null ? null : value.getIdentity();
  }

  @Override
  public int getObjectSize(OIdentifiable rid, Object... hints) {
    final ORID identity = rid.getIdentity();
    return OVarIntSerializer.sizeOfUnsigned(identity.getClusterId()) + OVarIntSerializer
        .sizeOfUnsigned(identity.getClusterPosition());
  }

  @Override
  public void serialize(OIdentifiable rid, byte[] stream, int startPosition, Object... hints) {
    serializeInByteBufferObject(rid, ByteBuffer.wrap(stream, startPosition, stream.length - startPosition), hints);
  }

  @Override
  public ORecordId deserialize(byte[] stream, int startPosition) {
    return deserializeFromByteBufferObject(ByteBuffer.wrap(stream, startPosition, stream.length - startPosition));
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return getObjectSizeInByteBuffer(ByteBuffer.wrap(stream, startPosition, stream.length - startPosition));
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return getObjectSize(stream, startPosition);
  }

  @Override
  public void serializeNativeObject(OIdentifiable rid, byte[] stream, int startPosition, Object... hints) {
    serialize(rid, stream, startPosition, hints);
  }

  @Override
  public ORecordId deserializeNativeObject(byte[] stream, int startPosition) {
    return deserialize(stream, startPosition);
  }

  @Override
  public void serializeInByteBufferObject(OIdentifiable object, ByteBuffer buffer, Object... hints) {
    final ORID identity = object.getIdentity();
    OVarIntSerializer.writeUnsigned(identity.getClusterId(), buffer);
    OVarIntSerializer.writeUnsigned(identity.getClusterPosition(), buffer);
  }

  @Override
  public ORecordId deserializeFromByteBufferObject(ByteBuffer buffer) {
    return new ORecordId(OVarIntSerializer.readUnsignedInteger(buffer), OVarIntSerializer.readUnsignedLong(buffer));
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return OVarIntSerializer.sizeOfSerializedValue(buffer) + OVarIntSerializer.sizeOfSerializedValue(buffer);
  }

  @Override
  public ORecordId deserializeFromByteBufferObject(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return new ORecordId(OVarIntSerializer.readUnsignedInteger(buffer, walChanges, offset),
        OVarIntSerializer.readUnsignedLong(buffer, walChanges, buffer.position()));
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return OVarIntSerializer.sizeOfSerializedValue(buffer, walChanges, offset) + OVarIntSerializer
        .sizeOfSerializedValue(buffer, walChanges, buffer.position());
  }
}
