package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import static com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinaryV0.readInteger;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinaryV0.readOType;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinaryV0.readString;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinaryV0.stringFromBytes;

import java.util.ArrayList;

import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class ORecordSerializerBinaryDebug {

  public ORecordSerializerBinaryDebug() {
  }

  public ORecordSerializationDebug deserializeDebug(final byte[] iSource, ODatabaseDocumentInternal db) {
    BytesContainer bytes = new BytesContainer(iSource);
    bytes.skip(1);
    switch (bytes.bytes[0]) {
    case 0:
      return deserializeDebugV0(bytes, db);
    case 1:
      return deserializeDebugV1(bytes, db);
    default:
      throw new OSystemException("Unsupported binary serialization version " + bytes.bytes[0]);
    }

  }

  public ORecordSerializationDebug deserializeDebugV1(BytesContainer bytes, ODatabaseDocumentInternal db) {
    ORecordSerializerBinaryV0 v1 = new ORecordSerializerBinaryV0();
    ORecordSerializationDebug debugInfo = new ORecordSerializationDebug();
    OImmutableSchema schema = db.getMetadata().getImmutableSchemaSnapshot();

    try {
      debugInfo.className = readString(bytes);
    } catch (RuntimeException ex) {
      debugInfo.readingFailure = true;
      debugInfo.readingException = ex;
      debugInfo.failPosition = bytes.offset;
      return debugInfo;
    }

    debugInfo.properties = new ArrayList<ORecordSerializationDebugProperty>();
    final int headerSize = OVarIntSerializer.readAsInteger(bytes);
    final int bodySize = OVarIntSerializer.readAsInteger(bytes);
    final int headerEnd = bytes.offset + headerSize;
    final int documentEnd = bytes.offset + headerSize + bodySize;

    String fieldName;
    int valueSize;
    int valuePos = bytes.offset + headerSize;
    BytesContainer body = new BytesContainer(bytes.bytes, valuePos);
    OType type = null;
    while (bytes.offset < headerEnd) {
      ORecordSerializationDebugProperty debugProperty = new ORecordSerializationDebugProperty();
      debugInfo.properties.add(debugProperty);
      OGlobalProperty prop = null;
      try {
        final int len = OVarIntSerializer.readAsInteger(bytes);
        if (len > 0) {
          // PARSE FIELD NAME
          fieldName = stringFromBytes(bytes.bytes, bytes.offset, len).intern();
          bytes.skip(len);
          valueSize = OVarIntSerializer.readAsInteger(bytes);
          type = readOType(bytes);
        } else {
          // LOAD GLOBAL PROPERTY BY ID
          final int id = (len * -1) - 1;
          debugProperty.globalId = id;
          prop = schema.getGlobalPropertyById(id);
          fieldName = prop.getName();
          valueSize = OVarIntSerializer.readAsInteger(bytes);
          if (prop.getType() != OType.ANY)
            type = prop.getType();
          else
            type = readOType(bytes);
        }
        int curPos = valuePos;
        valuePos += valueSize;
        debugProperty.valuePos = curPos;
        debugProperty.name = fieldName;
        debugProperty.type = type;

        if (valueSize > 0) {
          body.offset = curPos;
          try {
            debugProperty.value = v1.deserializeValue(body, type, new ODocument());
          } catch (RuntimeException ex) {
            debugProperty.faildToRead = true;
            debugProperty.readingException = ex;
            debugProperty.failPosition = bytes.offset;
          }
        } else {
          debugProperty.value = null;
        }
      } catch (RuntimeException ex) {
        debugInfo.readingFailure = true;
        debugInfo.readingException = ex;
        debugInfo.failPosition = bytes.offset;
        return debugInfo;
      }
    }

    return debugInfo;
  }

  public ORecordSerializationDebug deserializeDebugV0(BytesContainer bytes, ODatabaseDocumentInternal db) {
    ORecordSerializerBinaryV0 v0 = new ORecordSerializerBinaryV0();
    ORecordSerializationDebug debugInfo = new ORecordSerializationDebug();
    OImmutableSchema schema = db.getMetadata().getImmutableSchemaSnapshot();
    try {
      debugInfo.className = readString(bytes);
    } catch (RuntimeException ex) {
      debugInfo.readingFailure = true;
      debugInfo.readingException = ex;
      debugInfo.failPosition = bytes.offset;
      return debugInfo;
    }

    debugInfo.properties = new ArrayList<ORecordSerializationDebugProperty>();
    int last = 0;
    String fieldName;
    int valuePos;
    OType type;
    while (true) {
      ORecordSerializationDebugProperty debugProperty = new ORecordSerializationDebugProperty();
      OGlobalProperty prop;
      try {
        final int len = OVarIntSerializer.readAsInteger(bytes);
        if (len != 0)
          debugInfo.properties.add(debugProperty);
        if (len == 0) {
          // SCAN COMPLETED
          break;
        } else if (len > 0) {
          // PARSE FIELD NAME
          fieldName = stringFromBytes(bytes.bytes, bytes.offset, len).intern();
          bytes.skip(len);
          valuePos = readInteger(bytes);
          type = readOType(bytes);
        } else {
          // LOAD GLOBAL PROPERTY BY ID
          final int id = (len * -1) - 1;
          debugProperty.globalId = id;
          prop = schema.getGlobalPropertyById(id);
          valuePos = readInteger(bytes);
          if (prop != null) {
            fieldName = prop.getName();
            if (prop.getType() != OType.ANY)
              type = prop.getType();
            else
              type = readOType(bytes);
          } else {
            continue;
          }
        }
        debugProperty.valuePos = valuePos;
        debugProperty.name = fieldName;
        debugProperty.type = type;

        if (valuePos != 0) {
          int headerCursor = bytes.offset;
          bytes.offset = valuePos;
          try {
            debugProperty.value = v0.deserializeValue(bytes, type, new ODocument());
          } catch (RuntimeException ex) {
            debugProperty.faildToRead = true;
            debugProperty.readingException = ex;
            debugProperty.failPosition = bytes.offset;
          }
          if (bytes.offset > last)
            last = bytes.offset;
          bytes.offset = headerCursor;
        } else
          debugProperty.value = null;
      } catch (RuntimeException ex) {
        debugInfo.readingFailure = true;
        debugInfo.readingException = ex;
        debugInfo.failPosition = bytes.offset;
        return debugInfo;
      }
    }

    return debugInfo;
  }

}
