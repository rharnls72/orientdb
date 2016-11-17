package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;

import static com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinaryV0.*;

public class ORecordSerializerBinaryDebug implements ODocumentSerializer {

  private final ODocumentSerializer delegate;

  public ORecordSerializerBinaryDebug(ODocumentSerializer delegate) {
    this.delegate = delegate;
  }

  public ORecordSerializationDebug deserializeDebug(final byte[] iSource, ODatabaseDocumentInternal db) {
    ORecordSerializationDebug debugInfo = new ORecordSerializationDebug();
    OImmutableSchema schema = db.getMetadata().getImmutableSchemaSnapshot();
    BytesContainer bytes = new BytesContainer(iSource);
    if (bytes.bytes[0] != 0 && bytes.bytes[0] != 1)
      throw new OSystemException("Unsupported binary serialization version " + bytes.bytes[0]);
    bytes.skip(1);
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
          debugProperty.valuePos = valuePos;
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
        debugProperty.name = fieldName;
        debugProperty.type = type;

        if (valuePos != 0) {
          int headerCursor = bytes.offset;
          bytes.offset = valuePos;
          try {
            debugProperty.value = deserializeValue(bytes, type, new ODocument());
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

  @Override
  public void serialize(ODocument document, BytesContainer bytes, boolean iClassOnly) {
    delegate.serialize(document, bytes, iClassOnly);
  }

  @Override
  public int serializeValue(BytesContainer bytes, Object value, OType type, OType linkedType) {
    return delegate.serializeValue(bytes, value, type, linkedType);
  }

  @Override
  public void deserialize(ODocument document, BytesContainer bytes) {
    delegate.deserialize(document, bytes);
  }

  @Override
  public void deserializePartial(ODocument document, BytesContainer bytes, String[] iFields) {
    delegate.deserializePartial(document, bytes, iFields);
  }

  @Override
  public Object deserializeValue(BytesContainer bytes, OType type, ODocument ownerDocument) {
    return delegate.deserializeValue(bytes, type, ownerDocument);
  }

  @Override
  public OBinaryField deserializeField(BytesContainer bytes, OClass iClass, String iFieldName) {
    return delegate.deserializeField(bytes, iClass, iFieldName);
  }

  @Override
  public OBinaryComparator getComparator() {
    return delegate.getComparator();
  }

  @Override
  public String[] getFieldNames(ODocument reference, BytesContainer iBytes) {
    return delegate.getFieldNames(reference, iBytes);
  }

}
