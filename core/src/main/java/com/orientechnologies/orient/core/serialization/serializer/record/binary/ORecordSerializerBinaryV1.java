/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.*;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.util.ODateHelper;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;

public class ORecordSerializerBinaryV1 implements ODocumentSerializer {

  private static final String       CHARSET_UTF_8    = "UTF-8";
  private static final ORecordId    NULL_RECORD_ID   = new ORecordId(-2, ORID.CLUSTER_POS_INVALID);
  protected static final long       MILLISEC_PER_DAY = 86400000;

  // Keeping the V0 for now, no yet changes on comparable values
  private final OBinaryComparatorV0 comparator       = new OBinaryComparatorV0();

  public ORecordSerializerBinaryV1() {
  }

  public OBinaryComparator getComparator() {
    return comparator;
  }

  public void deserializePartial(final ODocument document, final BytesContainer bytes, final String[] iFields) {
    final String className = readString(bytes);
    if (className.length() != 0)
      ODocumentInternal.fillClassNameIfNeeded(document, className);

    // TRANSFORMS FIELDS FOM STRINGS TO BYTE[]
    final byte[][] fields = new byte[iFields.length][];
    for (int i = 0; i < iFields.length; ++i)
      fields[i] = iFields[i].getBytes();

    final int headerSize = OVarIntSerializer.readAsInteger(bytes);
    final int bodySize = OVarIntSerializer.readAsInteger(bytes);
    final int headerEnd = bytes.offset + headerSize;
    final int documentEnd = bytes.offset + headerSize + bodySize;

    String fieldName = null;
    int valueSize;
    int valuePos = bytes.offset + headerSize;
    BytesContainer body = new BytesContainer(bytes.bytes, valuePos);
    OType type = null;
    int unmarshalledFields = 0;

    while (bytes.offset < headerEnd) {
      final int len = OVarIntSerializer.readAsInteger(bytes);
      boolean match = false;
      if (len > 0) {
        // CHECK BY FIELD NAME SIZE: THIS AVOID EVEN THE UNMARSHALLING OF FIELD NAME
        for (int i = 0; i < iFields.length; ++i) {
          if (iFields[i] != null && iFields[i].length() == len) {
            boolean matchField = true;
            for (int j = 0; j < len; ++j) {
              if (bytes.bytes[bytes.offset + j] != fields[i][j]) {
                matchField = false;
                break;
              }
            }
            if (matchField) {
              fieldName = iFields[i];
              match = true;
              break;
            }
          }
        }
        bytes.skip(len);
        valueSize = OVarIntSerializer.readAsInteger(bytes);
        type = readOType(bytes);

      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final OGlobalProperty prop = getGlobalProperty(document, len);
        fieldName = prop.getName();

        for (String f : iFields) {
          if (fieldName.equals(f)) {
            match = true;
            break;
          }
        }

        valueSize = OVarIntSerializer.readAsInteger(bytes);
        if (prop.getType() != OType.ANY)
          type = prop.getType();
        else
          type = readOType(bytes);

      }
      int prePos = valuePos;
      valuePos += valueSize;

      if (!match) {
        // FIELD NOT INCLUDED: SKIP IT
        continue;
      }

      if (valueSize > 0) {
        body.offset = prePos;
        final Object value = deserializeValue(body, type, document);
        ODocumentInternal.rawField(document, fieldName, value, type);
      } else
        ODocumentInternal.rawField(document, fieldName, null, null);

      if (++unmarshalledFields == iFields.length)
        // ALL REQUESTED FIELDS UNMARSHALLED: EXIT
        break;
    }
    bytes.offset = documentEnd;
  }

  public OBinaryField deserializeField(final BytesContainer bytes, final OClass iClass, final String iFieldName) {
    // SKIP CLASS NAME
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);

    final byte[] field = iFieldName.getBytes();

    final OMetadataInternal metadata = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata();
    final OImmutableSchema _schema = metadata.getImmutableSchemaSnapshot();

    final int headerSize = OVarIntSerializer.readAsInteger(bytes);
    final int bodySize = OVarIntSerializer.readAsInteger(bytes);
    final int headerEnd = bytes.offset + headerSize;

    int valueSize;
    int valuePos = bytes.offset + headerSize;

    while (bytes.offset < headerEnd) {
      final int len = OVarIntSerializer.readAsInteger(bytes);
      if (len > 0) {
        // CHECK BY FIELD NAME SIZE: THIS AVOID EVEN THE UNMARSHALLING OF FIELD NAME
        int cursor = bytes.offset;
        bytes.skip(len);
        valueSize = OVarIntSerializer.readAsInteger(bytes);
        OType type = readOType(bytes);
        int prePos = valuePos;
        valuePos += valueSize;

        if (iFieldName.length() == len) {
          boolean match = true;
          for (int j = 0; j < len; ++j)
            if (bytes.bytes[cursor+j] != field[j]) {
              match = false;
              break;
            }

          if (!match)
            continue;

          if (valueSize == 0)
            return null;

          if (!ORecordSerializerBinary.INSTANCE.getCurrentSerializer().getComparator().isBinaryComparable(type))
            return null;

          bytes.offset = prePos;
          return new OBinaryField(iFieldName, type, bytes, null);
        }

      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final int id = (len * -1) - 1;
        final OGlobalProperty prop = _schema.getGlobalPropertyById(id);
        valueSize = OVarIntSerializer.readAsInteger(bytes);
        OType type = null;
        if (valueSize >= 0) {
          if (prop.getType() != OType.ANY)
            type = prop.getType();
          else
            type = readOType(bytes);
        }
        int prePos = valuePos;
        valuePos += valueSize;

        if (iFieldName.equals(prop.getName())) {
          if (valueSize == 0)
            return null;

          if (!ORecordSerializerBinary.INSTANCE.getCurrentSerializer().getComparator().isBinaryComparable(type))
            return null;

          bytes.offset = prePos;

          final OProperty classProp = iClass.getProperty(iFieldName);
          return new OBinaryField(iFieldName, type, bytes, classProp != null ? classProp.getCollate() : null);
        }
      }
    }
    return null;
  }

  @Override
  public void deserialize(final ODocument document, final BytesContainer bytes) {
    final String className = readString(bytes);
    if (className.length() != 0)
      ODocumentInternal.fillClassNameIfNeeded(document, className);

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
      OGlobalProperty prop = null;
      final int len = OVarIntSerializer.readAsInteger(bytes);
      if (len > 0) {
        // PARSE FIELD NAME
        fieldName = stringFromBytes(bytes.bytes, bytes.offset, len).intern();
        bytes.skip(len);
        valueSize = OVarIntSerializer.readAsInteger(bytes);
        type = readOType(bytes);
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        prop = getGlobalProperty(document, len);
        fieldName = prop.getName();
        valueSize = OVarIntSerializer.readAsInteger(bytes);
        if (prop.getType() != OType.ANY)
          type = prop.getType();
        else
          type = readOType(bytes);
      }
      int curPos = valuePos;
      valuePos += valueSize;

      if (ODocumentInternal.rawContainsField(document, fieldName)) {
        continue;
      }

      if (valueSize > 0) {
        body.offset = curPos;
        final Object value = deserializeValue(body, type, document);
        ODocumentInternal.rawField(document, fieldName, value, type);
      } else
        ODocumentInternal.rawField(document, fieldName, null, null);
    }

    ORecordInternal.clearSource(document);
    bytes.offset = documentEnd;
  }

  @Override
  public String[] getFieldNames(ODocument reference, final BytesContainer bytes) {
    // SKIP CLASS NAME
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);

    final int headerSize = OVarIntSerializer.readAsInteger(bytes);
    final int bodySize = OVarIntSerializer.readAsInteger(bytes);
    final int headerEnd = bytes.offset + headerSize;
    final int documentEnd = bytes.offset + headerSize + bodySize;

    final List<String> result = new ArrayList<String>();

    String fieldName;
    while (bytes.offset < headerEnd) {
      OGlobalProperty prop = null;
      final int len = OVarIntSerializer.readAsInteger(bytes);
      if (len > 0) {
        // PARSE FIELD NAME
        fieldName = stringFromBytes(bytes.bytes, bytes.offset, len).intern();
        bytes.skip(len);
        result.add(fieldName);
        
        // SKIP THE REST
        OVarIntSerializer.readAsInteger(bytes);
        readOType(bytes);
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final int id = (len * -1) - 1;
        prop = ODocumentInternal.getGlobalPropertyById(reference, id);
        if (prop == null) {
          throw new OSerializationException("Missing property definition for property id '" + id + "'");
        }
        result.add(prop.getName());

        // SKIP THE REST
        OVarIntSerializer.readAsInteger(bytes);
        if (prop.getType() == OType.ANY)
          readOType(bytes);
      }
    }

    return result.toArray(new String[result.size()]);
  }

  @Override
  public void serialize(final ODocument document, final BytesContainer bytes, final boolean iClassOnly) {
    final OClass clazz = serializeClass(document, bytes);
    if (iClassOnly) {
      OVarIntSerializer.write(bytes, 0);
      OVarIntSerializer.write(bytes, 0);
      return;
    }

    final Map<String, OProperty> props = clazz != null ? clazz.propertiesMap() : null;

    final Set<Entry<String, ODocumentEntry>> fields = ODocumentInternal.rawEntries(document);

    BytesContainer header = new BytesContainer();
    BytesContainer body = new BytesContainer();

    for (Entry<String, ODocumentEntry> entry : fields) {
      ODocumentEntry docEntry = entry.getValue();
      if (!docEntry.exist())
        continue;
      if (docEntry.property == null && props != null) {
        OProperty prop = props.get(entry.getKey());
        if (prop != null && docEntry.type == prop.getType())
          docEntry.property = prop;
      }

      boolean writeType;

      if (docEntry.property != null) {
        OVarIntSerializer.write(header, (docEntry.property.getId() + 1) * -1);
        if (docEntry.property.getType() == OType.ANY)
          writeType = true;
        else
          writeType = false;
      } else {
        writeString(header, entry.getKey());
        writeType = true;
      }

      final Object value = docEntry.value;
      if (value == null) {
        OVarIntSerializer.write(header, 0);
        if (writeType)
          header.bytes[header.alloc(1)] = -1;
      } else {
        int cur = body.offset;
        final OType type = getFieldType(docEntry);
        if (type == null) {
          throw new OSerializationException(
              "Impossible serialize value of type " + value.getClass() + " with the ODocument binary serializer");
        }
        serializeValue(body, value, type, getLinkedType(document, type, entry.getKey()));
        int size = body.offset - cur;
        OVarIntSerializer.write(header, size);
        if (writeType) {
          writeOType(header, header.alloc(1), type);
        }
      }
    }

    OVarIntSerializer.write(bytes, header.offset);
    OVarIntSerializer.write(bytes, body.offset);
    bytes.append(header);
    bytes.append(body);
  }

  public Object deserializeValue(final BytesContainer bytes, final OType type, final ODocument ownerDocument) {
    Object value = null;
    switch (type) {
    case INTEGER:
      value = OVarIntSerializer.readAsInteger(bytes);
      break;
    case LONG:
      value = OVarIntSerializer.readAsLong(bytes);
      break;
    case SHORT:
      value = OVarIntSerializer.readAsShort(bytes);
      break;
    case STRING:
      value = readString(bytes);
      break;
    case DOUBLE:
      value = Double.longBitsToDouble(readLong(bytes));
      break;
    case FLOAT:
      value = Float.intBitsToFloat(readInteger(bytes));
      break;
    case BYTE:
      value = readByte(bytes);
      break;
    case BOOLEAN:
      value = readByte(bytes) == 1;
      break;
    case DATETIME:
      value = new Date(OVarIntSerializer.readAsLong(bytes));
      break;
    case DATE:
      long savedTime = OVarIntSerializer.readAsLong(bytes) * MILLISEC_PER_DAY;
      savedTime = convertDayToTimezone(TimeZone.getTimeZone("GMT"), ODateHelper.getDatabaseTimeZone(), savedTime);
      value = new Date(savedTime);
      break;
    case EMBEDDED:
      value = new ODocument();
      deserialize((ODocument) value, bytes);
      if (((ODocument) value).containsField(ODocumentSerializable.CLASS_NAME)) {
        String className = ((ODocument) value).field(ODocumentSerializable.CLASS_NAME);
        try {
          Class<?> clazz = Class.forName(className);
          ODocumentSerializable newValue = (ODocumentSerializable) clazz.newInstance();
          newValue.fromDocument((ODocument) value);
          value = newValue;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else
        ODocumentInternal.addOwner((ODocument) value, ownerDocument);

      break;
    case EMBEDDEDSET:
      value = readEmbeddedSet(bytes, ownerDocument);
      break;
    case EMBEDDEDLIST:
      value = readEmbeddedList(bytes, ownerDocument);
      break;
    case LINKSET:
      value = readLinkCollection(bytes, new ORecordLazySet(ownerDocument));
      break;
    case LINKLIST:
      value = readLinkCollection(bytes, new ORecordLazyList(ownerDocument));
      break;
    case BINARY:
      value = readBinary(bytes);
      break;
    case LINK:
      value = readOptimizedLink(bytes);
      break;
    case LINKMAP:
      value = readLinkMap(bytes, ownerDocument);
      break;
    case EMBEDDEDMAP:
      value = readEmbeddedMap(bytes, ownerDocument);
      break;
    case DECIMAL:
      value = ODecimalSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
      bytes.skip(ODecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
      break;
    case LINKBAG:
      ORidBag bag = new ORidBag();
      bag.deserializeCompact(bytes);
      bag.setOwner(ownerDocument);
      value = bag;
      break;
    case TRANSIENT:
      break;
    case CUSTOM:
      try {
        String className = readString(bytes);
        Class<?> clazz = Class.forName(className);
        OSerializableStream stream = (OSerializableStream) clazz.newInstance();
        stream.fromStream(readBinary(bytes));
        if (stream instanceof OSerializableWrapper)
          value = ((OSerializableWrapper) stream).getSerializable();
        else
          value = stream;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      break;
    case ANY:
      break;

    }
    return value;
  }

  protected OClass serializeClass(final ODocument document, final BytesContainer bytes) {
    final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    if (clazz != null)
      writeString(bytes, clazz.getName());
    else
      writeEmptyString(bytes);
    return clazz;
  }

  protected OGlobalProperty getGlobalProperty(final ODocument document, final int len) {
    final int id = (len * -1) - 1;
    return ODocumentInternal.getGlobalPropertyById(document, id);
  }

  protected static OType readOType(final BytesContainer bytes) {
    return OType.getById(readByte(bytes));
  }

  private void writeOType(BytesContainer bytes, int pos, OType type) {
    bytes.bytes[pos] = (byte) type.getId();
  }

  protected static byte[] readBinary(final BytesContainer bytes) {
    final int n = OVarIntSerializer.readAsInteger(bytes);
    final byte[] newValue = new byte[n];
    System.arraycopy(bytes.bytes, bytes.offset, newValue, 0, newValue.length);
    bytes.skip(n);
    return newValue;
  }

  protected Map<Object, OIdentifiable> readLinkMap(final BytesContainer bytes, final ODocument document) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final ORecordLazyMap result = new ORecordLazyMap(document);

    result.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
    try {

      while ((size--) > 0) {
        final OType keyType = readOType(bytes);
        final Object key = deserializeValue(bytes, keyType, document);
        final ORecordId value = readOptimizedLink(bytes);
        if (value.equals(NULL_RECORD_ID))
          result.put(key, null);
        else
          result.put(key, value);
      }
      return result;

    } finally {
      result.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
  }

  protected Object readEmbeddedMap(final BytesContainer bytes, final ODocument document) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final OTrackedMap<Object> result = new OTrackedMap<Object>(document);
    result.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
    try {
      while ((size--) > 0) {
        OType keyType = readOType(bytes);
        Object key = deserializeValue(bytes, keyType, document);
        byte id = readByte(bytes);
        if (id >= 0) {
          final OType type = OType.getById(id);
          Object value = deserializeValue(bytes, type, document);
          result.put(key, value);
        } else
          result.put(key, null);
      }
      return result;
    } finally {
      result.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
  }

  protected Collection<OIdentifiable> readLinkCollection(final BytesContainer bytes, final Collection<OIdentifiable> found) {
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      ORecordId id = readOptimizedLink(bytes);
      if (id.equals(NULL_RECORD_ID))
        found.add(null);
      else
        found.add(id);
    }
    return found;
  }

  protected static ORecordId readOptimizedLink(final BytesContainer bytes) {
    return new ORecordId(OVarIntSerializer.readAsInteger(bytes), OVarIntSerializer.readAsLong(bytes));
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected Collection<?> readEmbeddedSet(final BytesContainer bytes, final ODocument ownerDocument) {

    final int items = OVarIntSerializer.readAsInteger(bytes);
    OType type = readOType(bytes);

    if (type == OType.ANY) {
      final OTrackedSet found = new OTrackedSet<Object>(ownerDocument);

      found.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
      try {

        for (int i = 0; i < items; i++) {
          byte itemType = readByte(bytes);
          if (itemType == -1)
            found.add(null);
          else
            found.add(deserializeValue(bytes, OType.getById(itemType), ownerDocument));
        }
        return found;

      } finally {
        found.setInternalStatus(ORecordElement.STATUS.LOADED);
      }
    }
    // TODO: manage case where type is known
    return null;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected Collection<?> readEmbeddedList(final BytesContainer bytes, final ODocument ownerDocument) {

    final int items = OVarIntSerializer.readAsInteger(bytes);
    OType type = readOType(bytes);

    if (type == OType.ANY) {
      final OTrackedList found = new OTrackedList<Object>(ownerDocument);

      found.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
      try {

        for (int i = 0; i < items; i++) {
          byte itemType = readByte(bytes);
          if (itemType == -1)
            found.add(null);
          else
            found.add(deserializeValue(bytes, OType.getById(itemType), ownerDocument));
        }
        return found;

      } finally {
        found.setInternalStatus(ORecordElement.STATUS.LOADED);
      }
    }
    // TODO: manage case where type is known
    return null;
  }

  private OType getLinkedType(ODocument document, OType type, String key) {
    if (type != OType.EMBEDDEDLIST && type != OType.EMBEDDEDSET && type != OType.EMBEDDEDMAP)
      return null;
    OClass immutableClass = ODocumentInternal.getImmutableSchemaClass(document);
    if (immutableClass != null) {
      OProperty prop = immutableClass.getProperty(key);
      if (prop != null) {
        return prop.getLinkedType();
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public int serializeValue(final BytesContainer bytes, Object value, final OType type, final OType linkedType) {
    int pointer = 0;
    switch (type) {
    case INTEGER:
    case LONG:
    case SHORT:
      pointer = OVarIntSerializer.write(bytes, ((Number) value).longValue());
      break;
    case STRING:
      pointer = writeString(bytes, value.toString());
      break;
    case DOUBLE:
      long dg = Double.doubleToLongBits(((Number) value).doubleValue());
      pointer = bytes.alloc(OLongSerializer.LONG_SIZE);
      OLongSerializer.INSTANCE.serializeLiteral(dg, bytes.bytes, pointer);
      break;
    case FLOAT:
      int fg = Float.floatToIntBits(((Number) value).floatValue());
      pointer = bytes.alloc(OIntegerSerializer.INT_SIZE);
      OIntegerSerializer.INSTANCE.serializeLiteral(fg, bytes.bytes, pointer);
      break;
    case BYTE:
      pointer = bytes.alloc(1);
      bytes.bytes[pointer] = ((Number) value).byteValue();
      break;
    case BOOLEAN:
      pointer = bytes.alloc(1);
      bytes.bytes[pointer] = ((Boolean) value) ? (byte) 1 : (byte) 0;
      break;
    case DATETIME:
      if (value instanceof Number) {
        pointer = OVarIntSerializer.write(bytes, ((Number) value).longValue());
      } else
        pointer = OVarIntSerializer.write(bytes, ((Date) value).getTime());
      break;
    case DATE:
      long dateValue;
      if (value instanceof Number) {
        dateValue = ((Number) value).longValue();
      } else
        dateValue = ((Date) value).getTime();
      dateValue = convertDayToTimezone(ODateHelper.getDatabaseTimeZone(), TimeZone.getTimeZone("GMT"), dateValue);
      pointer = OVarIntSerializer.write(bytes, dateValue / MILLISEC_PER_DAY);
      break;
    case EMBEDDED:
      pointer = bytes.offset;
      if (value instanceof ODocumentSerializable) {
        ODocument cur = ((ODocumentSerializable) value).toDocument();
        cur.field(ODocumentSerializable.CLASS_NAME, value.getClass().getName());
        serialize(cur, bytes, false);
      } else {
        serialize((ODocument) value, bytes, false);
      }
      break;
    case EMBEDDEDSET:
    case EMBEDDEDLIST:
      if (value.getClass().isArray())
        pointer = writeEmbeddedCollection(bytes, Arrays.asList(OMultiValue.array(value)), linkedType);
      else
        pointer = writeEmbeddedCollection(bytes, (Collection<?>) value, linkedType);
      break;
    case DECIMAL:
      BigDecimal decimalValue = (BigDecimal) value;
      pointer = bytes.alloc(ODecimalSerializer.INSTANCE.getObjectSize(decimalValue));
      ODecimalSerializer.INSTANCE.serialize(decimalValue, bytes.bytes, pointer);
      break;
    case BINARY:
      pointer = writeBinary(bytes, (byte[]) (value));
      break;
    case LINKSET:
    case LINKLIST:
      Collection<OIdentifiable> ridCollection = (Collection<OIdentifiable>) value;
      pointer = writeLinkCollection(bytes, ridCollection);
      break;
    case LINK:
      if (!(value instanceof OIdentifiable))
        throw new OValidationException("Value '" + value + "' is not a OIdentifiable");

      pointer = writeOptimizedLink(bytes, (OIdentifiable) value);
      break;
    case LINKMAP:
      pointer = writeLinkMap(bytes, (Map<Object, OIdentifiable>) value);
      break;
    case EMBEDDEDMAP:
      pointer = writeEmbeddedMap(bytes, (Map<Object, Object>) value);
      break;
    case LINKBAG:
      pointer = ((ORidBag) value).serializeCompact(bytes);
      break;
    case CUSTOM:
      if (!(value instanceof OSerializableStream))
        value = new OSerializableWrapper((Serializable) value);
      pointer = writeString(bytes, value.getClass().getName());
      writeBinary(bytes, ((OSerializableStream) value).toStream());
      break;
    case TRANSIENT:
      break;
    case ANY:
      break;
    }
    return pointer;
  }

  protected int writeBinary(final BytesContainer bytes, final byte[] valueBytes) {
    final int pointer = OVarIntSerializer.write(bytes, valueBytes.length);
    final int start = bytes.alloc(valueBytes.length);
    System.arraycopy(valueBytes, 0, bytes.bytes, start, valueBytes.length);
    return pointer;
  }

  protected int writeLinkMap(final BytesContainer bytes, final Map<Object, OIdentifiable> map) {
    final boolean disabledAutoConversion = map instanceof ORecordLazyMultiValue
        && ((ORecordLazyMultiValue) map).isAutoConvertToRecord();

    if (disabledAutoConversion)
      // AVOID TO FETCH RECORD
      ((ORecordLazyMultiValue) map).setAutoConvertToRecord(false);

    try {
      final int fullPos = OVarIntSerializer.write(bytes, map.size());
      for (Entry<Object, OIdentifiable> entry : map.entrySet()) {
        // TODO:check skip of complex types
        // FIXME: changed to support only string key on map
        final OType type = OType.STRING;
        writeOType(bytes, bytes.alloc(1), type);
        writeString(bytes, entry.getKey().toString());
        if (entry.getValue() == null)
          writeNullLink(bytes);
        else
          writeOptimizedLink(bytes, entry.getValue());
      }
      return fullPos;

    } finally {
      if (disabledAutoConversion)
        ((ORecordLazyMultiValue) map).setAutoConvertToRecord(true);
    }
  }

  protected int writeEmbeddedMap(BytesContainer bytes, Map<Object, Object> map) {
    final int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Entry<Object, Object> entry : map.entrySet()) {
      writeOType(bytes, bytes.alloc(1), OType.STRING);
      writeString(bytes, entry.getKey().toString());
      if (entry.getValue() == null) {
        bytes.bytes[bytes.alloc(1)] = -1;
      } else {
        final OType valueType = getTypeFromValueEmbedded(entry.getValue());
        if (valueType == null) {
          throw new OSerializationException(
              "Impossible serialize value of type " + entry.getValue().getClass() + " with the ODocument binary serializer");
        }
        writeOType(bytes, bytes.alloc(1), valueType);
        serializeValue(bytes, entry.getValue(), valueType, null);
      }
    }
    return fullPos;
  }

  private int writeNullLink(final BytesContainer bytes) {
    final int pos = OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterPosition());
    return pos;

  }

  protected int writeOptimizedLink(final BytesContainer bytes, OIdentifiable link) {
    if (!link.getIdentity().isPersistent()) {
      try {
        final ORecord real = link.getRecord();
        if (real != null)
          link = real;
      } catch (ORecordNotFoundException ex) {
        // IGNORE IT WILL FAIL THE ASSERT IN CASE
      }
    }
    if (link.getIdentity().getClusterId() < 0 && ORecordSerializationContext.getContext() != null)
      throw new ODatabaseException("Impossible to serialize invalid link " + link.getIdentity());

    final int pos = OVarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
    return pos;
  }

  protected int writeLinkCollection(final BytesContainer bytes, final Collection<OIdentifiable> value) {
    final int pos = OVarIntSerializer.write(bytes, value.size());

    final boolean disabledAutoConversion = value instanceof ORecordLazyMultiValue
        && ((ORecordLazyMultiValue) value).isAutoConvertToRecord();

    if (disabledAutoConversion)
      // AVOID TO FETCH RECORD
      ((ORecordLazyMultiValue) value).setAutoConvertToRecord(false);

    try {
      for (OIdentifiable itemValue : value) {
        // TODO: handle the null links
        if (itemValue == null)
          writeNullLink(bytes);
        else
          writeOptimizedLink(bytes, itemValue);
      }

    } finally {
      if (disabledAutoConversion)
        ((ORecordLazyMultiValue) value).setAutoConvertToRecord(true);
    }

    return pos;
  }

  protected int writeEmbeddedCollection(final BytesContainer bytes, final Collection<?> value, final OType linkedType) {
    final int pos = OVarIntSerializer.write(bytes, value.size());
    // TODO manage embedded type from schema and auto-determined.
    writeOType(bytes, bytes.alloc(1), OType.ANY);
    for (Object itemValue : value) {
      // TODO:manage in a better way null entry
      if (itemValue == null) {
        bytes.bytes[bytes.alloc(1)] = -1;
        continue;
      }
      OType type;
      if (linkedType == null || linkedType == OType.ANY)
        type = getTypeFromValueEmbedded(itemValue);
      else
        type = linkedType;
      if (type != null) {
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(bytes, itemValue, type, null);
      } else {
        throw new OSerializationException(
            "Impossible serialize value of type " + itemValue.getClass() + " with the ODocument binary serializer");
      }
    }
    return pos;
  }

  private OType getFieldType(final ODocumentEntry entry) {
    OType type = entry.type;
    if (type == null) {
      final OProperty prop = entry.property;
      if (prop != null)
        type = prop.getType();

    }
    if (type == null || OType.ANY == type)
      type = OType.getTypeByValue(entry.value);
    return type;
  }

  private OType getTypeFromValueEmbedded(final Object fieldValue) {
    OType type = OType.getTypeByValue(fieldValue);
    if (type == OType.LINK && fieldValue instanceof ODocument && !((ODocument) fieldValue).getIdentity().isValid())
      type = OType.EMBEDDED;
    return type;
  }

  protected static String readString(final BytesContainer bytes) {
    final int len = OVarIntSerializer.readAsInteger(bytes);
    final String res = stringFromBytes(bytes.bytes, bytes.offset, len);
    bytes.skip(len);
    return res;
  }

  protected static int readInteger(final BytesContainer container) {
    final int value = OIntegerSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OIntegerSerializer.INT_SIZE;
    return value;
  }

  protected static byte readByte(final BytesContainer container) {
    return container.bytes[container.offset++];
  }

  protected static long readLong(final BytesContainer container) {
    final long value = OLongSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OLongSerializer.LONG_SIZE;
    return value;
  }

  private int writeEmptyString(final BytesContainer bytes) {
    return OVarIntSerializer.write(bytes, 0);
  }

  protected int writeString(final BytesContainer bytes, final String toWrite) {
    final byte[] nameBytes = bytesFromString(toWrite);
    final int pointer = OVarIntSerializer.write(bytes, nameBytes.length);
    final int start = bytes.alloc(nameBytes.length);
    System.arraycopy(nameBytes, 0, bytes.bytes, start, nameBytes.length);
    return pointer;
  }

  private byte[] bytesFromString(final String toWrite) {
    try {
      return toWrite.getBytes(CHARSET_UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("Error on string encoding"), e);
    }
  }

  protected static String stringFromBytes(final byte[] bytes, final int offset, final int len) {
    try {
      return new String(bytes, offset, len, CHARSET_UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("Error on string decoding"), e);
    }
  }

  protected static long convertDayToTimezone(TimeZone from, TimeZone to, long time) {
    Calendar fromCalendar = Calendar.getInstance(from);
    fromCalendar.setTimeInMillis(time);
    Calendar toCalendar = Calendar.getInstance(to);
    toCalendar.setTimeInMillis(0);
    toCalendar.set(Calendar.ERA, fromCalendar.get(Calendar.ERA));
    toCalendar.set(Calendar.YEAR, fromCalendar.get(Calendar.YEAR));
    toCalendar.set(Calendar.MONTH, fromCalendar.get(Calendar.MONTH));
    toCalendar.set(Calendar.DAY_OF_MONTH, fromCalendar.get(Calendar.DAY_OF_MONTH));
    toCalendar.set(Calendar.HOUR_OF_DAY, 0);
    toCalendar.set(Calendar.MINUTE, 0);
    toCalendar.set(Calendar.SECOND, 0);
    toCalendar.set(Calendar.MILLISECOND, 0);
    return toCalendar.getTimeInMillis();
  }

}
