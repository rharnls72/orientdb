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

package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import com.orientechnologies.common.comparator.OComparatorFactory;
import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.*;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OSBTreeBonsaiLocalException;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OVarLinkSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @author Andrey Lomakin
 * @author Sergey Sitnikov – binary version, variable-sized key serializers, ID and short pointers support
 * @since 8/7/13
 */
public class OSBTreeBonsaiBucket<K, V> extends OBonsaiBucketAbstract {
  public static final int MAX_BUCKET_SIZE_BYTES = OGlobalConfiguration.SBTREEBONSAI_BUCKET_SIZE.getValueAsInteger() * 1024;

  /**
   * The possible results of {@link #updateValue(int, Object)} call.
   */
  public enum UpdateResult {
    /**
     * Indicates that the new value passed is identical to the stored one, no update was performed.
     */
    NoChange,

    /**
     * Indicates that the stored value was updated.
     */
    Updated,

    /**
     * Indicates that the new value is of a different size than the stored one and it's not updated. For example, the caller may
     * handle this situation by removing the key and inserting it back with the new value.
     */
    Reinsert
  }

  private static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR = OComparatorFactory.INSTANCE.getComparator(byte[].class);

  private static final byte VERSION_1 = 0;
  private static final byte VERSION_2 = 1;

  private static final byte LEAF_MASK     = 0x01; // 0b0000_0001
  private static final byte DELETED_MASK  = 0x02; // 0b0000_0010
  private static final byte VERSION_MASK  = 0x3C; // 0b0011_1100
  private static final int  VERSION_SHIFT = 2;

  private static final int FREE_POINTER_OFFSET      = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int SIZE_OFFSET              = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int FLAGS_OFFSET             = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int FREE_LIST_POINTER_OFFSET = FLAGS_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int LEFT_SIBLING_OFFSET      = FREE_LIST_POINTER_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int RIGHT_SIBLING_OFFSET     = LEFT_SIBLING_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int TREE_SIZE_OFFSET         = RIGHT_SIBLING_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int KEY_SERIALIZER_OFFSET    = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int VALUE_SERIALIZER_OFFSET  = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;

  private final int ID_OFFSET;
  private final int POSITIONS_ARRAY_OFFSET;

  private static final int MAX_ENTREE_SIZE =
      MAX_BUCKET_SIZE_BYTES - VALUE_SERIALIZER_OFFSET /* fixed headers */ - OByteSerializer.BYTE_SIZE /* value serializer ID */
          - OLongSerializer.LONG_SIZE /* tree ID */ - OIntegerSerializer.INT_SIZE /* entry position */
          - OBonsaiBucketPointer.SIZE * 2 /* non-leaf pointers */;

  private final boolean isLeaf;
  private final byte    version;
  private final int     offset;

  private final OBinarySerializer<K> keySerializer;
  private final OBinarySerializer<V> valueSerializer;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  private final OSBTreeBonsaiLocal<K, V> tree;

  public static final class SBTreeEntry<K, V> implements Map.Entry<K, V>, Comparable<SBTreeEntry<K, V>> {
    public final OBonsaiBucketPointer leftChild;
    public final OBonsaiBucketPointer rightChild;
    public final K                    key;
    public final V                    value;
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    public SBTreeEntry(OBonsaiBucketPointer leftChild, OBonsaiBucketPointer rightChild, K key, V value) {
      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException("SBTreeEntry.setValue");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      SBTreeEntry that = (SBTreeEntry) o;

      if (!leftChild.equals(that.leftChild))
        return false;
      if (!rightChild.equals(that.rightChild))
        return false;
      if (!key.equals(that.key))
        return false;
      if (value != null ? !value.equals(that.value) : that.value != null)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = leftChild.hashCode();
      result = 31 * result + rightChild.hashCode();
      result = 31 * result + key.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "SBTreeEntry{" + "leftChild=" + leftChild + ", rightChild=" + rightChild + ", key=" + key + ", value=" + value + '}';
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public int compareTo(SBTreeEntry<K, V> other) {
      return comparator.compare(key, other.key);
    }
  }

  public OSBTreeBonsaiBucket(OCacheEntry cacheEntry, int pageOffset, boolean isLeaf, OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer, OWALChanges changes, OSBTreeBonsaiLocal<K, V> tree) throws IOException {
    super(cacheEntry, changes);

    this.offset = pageOffset;
    this.isLeaf = isLeaf;
    this.version = VERSION_2;

    setIntValue(offset + FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    setIntValue(offset + SIZE_OFFSET, 0);

    //THIS REMOVE ALSO THE EVENTUAL DELETED FLAG
    setByteValue(offset + FLAGS_OFFSET, encodeVersion(encodeFlag((byte) 0, LEAF_MASK, isLeaf), version));
    setLongValue(offset + LEFT_SIBLING_OFFSET, -1);
    setLongValue(offset + RIGHT_SIBLING_OFFSET, -1);

    setLongValue(offset + TREE_SIZE_OFFSET, 0);

    setKeySerializerId(keySerializer.getId());
    setValueSerializerId(valueSerializer.getId());

    this.keySerializer = upgradeSerializer(keySerializer, version);
    this.valueSerializer = upgradeSerializer(valueSerializer, version);

    ID_OFFSET = VALUE_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;
    POSITIONS_ARRAY_OFFSET = ID_OFFSET + OLongSerializer.LONG_SIZE;

    this.tree = tree;
  }

  public OSBTreeBonsaiBucket(OCacheEntry cacheEntry, int pageOffset, OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer, OWALChanges changes, OSBTreeBonsaiLocal<K, V> tree) {
    super(cacheEntry, changes);

    this.offset = pageOffset;

    final byte flags = getByteValue(offset + FLAGS_OFFSET);
    this.isLeaf = decodeFlag(flags, LEAF_MASK);
    this.version = decodeVersion(flags);

    this.keySerializer = upgradeSerializer(keySerializer, version);
    this.valueSerializer = upgradeSerializer(valueSerializer, version);
    this.tree = tree;

    ID_OFFSET = version == VERSION_1 ? -1 : VALUE_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;
    POSITIONS_ARRAY_OFFSET =
        version == VERSION_1 ? VALUE_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE : ID_OFFSET + OLongSerializer.LONG_SIZE;
  }

  public byte getKeySerializerId() {
    return getByteValue(offset + KEY_SERIALIZER_OFFSET);
  }

  public void setKeySerializerId(byte keySerializerId) {
    setByteValue(offset + KEY_SERIALIZER_OFFSET, keySerializerId);
  }

  public byte getValueSerializerId() {
    return getByteValue(offset + VALUE_SERIALIZER_OFFSET);
  }

  public void setValueSerializerId(byte valueSerializerId) {
    setByteValue(offset + VALUE_SERIALIZER_OFFSET, valueSerializerId);
  }

  public long getTreeSize() {
    return getLongValue(offset + TREE_SIZE_OFFSET);
  }

  public void setTreeSize(long size) throws IOException {
    setLongValue(offset + TREE_SIZE_OFFSET, size);
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public int find(K key) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      K midVal = getKey(mid);
      int cmp = comparator.compare(midVal, key);

      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return mid; // key found
    }
    return -(low + 1); // key not found.
  }

  public void remove(int entryIndex) throws IOException {
    final int positionSize = getPositionSize();

    int entryPosition = getPosition(entryIndex);

    int entrySize = getObjectSizeInDirectMemory(keySerializer, offset + entryPosition);
    if (isLeaf) {
      entrySize += getObjectSizeInDirectMemory(valueSerializer, offset + entryPosition + entrySize);
    } else {
      throw new IllegalStateException("Remove is applies to leaf buckets only");
    }

    int size = size();
    if (entryIndex < size - 1) {
      moveData(offset + POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * positionSize,
          offset + POSITIONS_ARRAY_OFFSET + entryIndex * positionSize, (size - entryIndex - 1) * positionSize);
    }

    size--;
    setIntValue(offset + SIZE_OFFSET, size);

    int freePointer = getIntValue(offset + FREE_POINTER_OFFSET);
    if (size > 0 && entryPosition > freePointer) {
      moveData(offset + freePointer, offset + freePointer + entrySize, entryPosition - freePointer);
    }
    setIntValue(offset + FREE_POINTER_OFFSET, freePointer + entrySize);

    int currentPositionOffset = offset + POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      int currentEntryPosition = readPosition(currentPositionOffset);
      if (currentEntryPosition < entryPosition)
        writePosition(currentPositionOffset, currentEntryPosition + entrySize);
      currentPositionOffset += positionSize;
    }
  }

  public int size() {
    return getIntValue(offset + SIZE_OFFSET);
  }

  public SBTreeEntry<K, V> getEntry(int entryIndex) {
    int entryPosition = getPosition(entryIndex);

    if (isLeaf) {
      K key = deserializeFromDirectMemory(keySerializer, offset + entryPosition);
      entryPosition += getObjectSizeInDirectMemory(keySerializer, offset + entryPosition);

      V value = deserializeFromDirectMemory(valueSerializer, offset + entryPosition);

      return new SBTreeEntry<K, V>(OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL, key, value);
    } else {
      final int pointerSize = getPointerSize();

      OBonsaiBucketPointer leftChild = getBucketPointer(offset + entryPosition);
      entryPosition += pointerSize;

      OBonsaiBucketPointer rightChild = getBucketPointer(offset + entryPosition);
      entryPosition += pointerSize;

      K key = deserializeFromDirectMemory(keySerializer, offset + entryPosition);

      return new SBTreeEntry<K, V>(leftChild, rightChild, key, null);
    }
  }

  public K getKey(int index) {
    int entryPosition = getPosition(index);

    if (!isLeaf)
      entryPosition += 2 * getPointerSize();

    return deserializeFromDirectMemory(keySerializer, offset + entryPosition);
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public void addAll(List<SBTreeEntry<K, V>> entries) throws IOException {
    for (int i = 0; i < entries.size(); i++)
      addEntry(i, entries.get(i), false);
  }

  public void shrink(int newSize) throws IOException {
    List<SBTreeEntry<K, V>> treeEntries = new ArrayList<SBTreeEntry<K, V>>(newSize);

    for (int i = 0; i < newSize; i++) {
      treeEntries.add(getEntry(i));
    }

    setIntValue(offset + FREE_POINTER_OFFSET, MAX_BUCKET_SIZE_BYTES);
    setIntValue(offset + SIZE_OFFSET, 0);

    int index = 0;
    for (SBTreeEntry<K, V> entry : treeEntries) {
      addEntry(index, entry, false);
      index++;
    }
  }

  public boolean addEntry(int index, SBTreeEntry<K, V> treeEntry, boolean updateNeighbors) throws IOException {
    final int positionSize = getPositionSize();
    final int pointerSize = getPointerSize();

    final int keySize = keySerializer.getObjectSize(treeEntry.key);
    int valueSize = 0;
    int entrySize = keySize;

    if (isLeaf) {
      valueSize = valueSerializer.getObjectSize(treeEntry.value);

      entrySize += valueSize;

      checkEntreeSize(entrySize);
    } else
      entrySize += 2 * pointerSize;

    int size = size();
    int freePointer = getIntValue(offset + FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * positionSize + POSITIONS_ARRAY_OFFSET) {
      if (size > 1)
        return false;
      else
        throw new OSBTreeBonsaiLocalException(
            "Entry size ('key + value') is more than is more than allowed " + (freePointer - 2 * OIntegerSerializer.INT_SIZE
                + POSITIONS_ARRAY_OFFSET) + " bytes, either increase page size using '"
                + OGlobalConfiguration.SBTREEBONSAI_BUCKET_SIZE.getKey() + "' parameter, or decrease 'key + value' size.", tree);
    }

    if (index <= size - 1)
      moveData(offset + POSITIONS_ARRAY_OFFSET + index * positionSize, offset + POSITIONS_ARRAY_OFFSET + (index + 1) * positionSize,
          (size - index) * positionSize);

    freePointer -= entrySize;

    setIntValue(offset + FREE_POINTER_OFFSET, freePointer);
    setPosition(index, freePointer);
    setIntValue(offset + SIZE_OFFSET, size + 1);

    if (isLeaf) {
      byte[] serializedKey = new byte[keySize];
      keySerializer.serializeNativeObject(treeEntry.key, serializedKey, 0);

      setBinaryValue(offset + freePointer, serializedKey);
      freePointer += keySize;

      byte[] serializedValue = new byte[valueSize];
      valueSerializer.serializeNativeObject(treeEntry.value, serializedValue, 0);
      setBinaryValue(offset + freePointer, serializedValue);
    } else {
      setBucketPointer(offset + freePointer, treeEntry.leftChild);
      freePointer += pointerSize;

      setBucketPointer(offset + freePointer, treeEntry.rightChild);
      freePointer += pointerSize;

      byte[] serializedKey = new byte[keySize];
      keySerializer.serializeNativeObject(treeEntry.key, serializedKey, 0);
      setBinaryValue(offset + freePointer, serializedKey);

      size++;

      if (updateNeighbors && size > 1) {
        if (index < size - 1) {
          final int nextEntryPosition = getPosition(index + 1);
          setBucketPointer(offset + nextEntryPosition, treeEntry.rightChild);
        }

        if (index > 0) {
          final int prevEntryPosition = getPosition(index - 1);
          setBucketPointer(offset + prevEntryPosition + pointerSize, treeEntry.leftChild);
        }
      }
    }

    return true;
  }

  public UpdateResult updateValue(int index, V value) throws IOException {
    int entryPosition = getPosition(index);
    final int keySize = getObjectSizeInDirectMemory(keySerializer, offset + entryPosition);
    entryPosition += keySize;

    final int oldSize = getObjectSizeInDirectMemory(valueSerializer, offset + entryPosition);
    final int newSize = valueSerializer.getObjectSize(value);

    if (oldSize != newSize) {
      checkEntreeSize(keySize + newSize);
      return UpdateResult.Reinsert;
    }

    final byte[] oldBytes = getBinaryValue(offset + entryPosition, oldSize);
    final byte[] newBytes = new byte[newSize];
    valueSerializer.serializeNativeObject(value, newBytes, 0);

    if (BYTE_ARRAY_COMPARATOR.compare(oldBytes, newBytes) == 0)
      return UpdateResult.NoChange;

    setBinaryValue(offset + entryPosition, newBytes);
    return UpdateResult.Updated;
  }

  public OBonsaiBucketPointer getFreeListPointer() {
    return super.getBucketPointer(offset + FREE_LIST_POINTER_OFFSET);
  }

  public void setFreeListPointer(OBonsaiBucketPointer pointer) throws IOException {
    super.setBucketPointer(offset + FREE_LIST_POINTER_OFFSET, pointer);
  }

  public void setDeleted(boolean deleted) {
    final byte flags = getByteValue(offset + FLAGS_OFFSET);
    setByteValue(offset + FLAGS_OFFSET, encodeFlag(flags, DELETED_MASK, deleted));
  }

  public boolean isDeleted() {
    return decodeFlag(getByteValue(offset + FLAGS_OFFSET), DELETED_MASK);
  }

  public OBonsaiBucketPointer getLeftSibling() {
    return super.getBucketPointer(offset + LEFT_SIBLING_OFFSET);
  }

  public void setLeftSibling(OBonsaiBucketPointer pointer) throws IOException {
    super.setBucketPointer(offset + LEFT_SIBLING_OFFSET, pointer);
  }

  public OBonsaiBucketPointer getRightSibling() {
    return super.getBucketPointer(offset + RIGHT_SIBLING_OFFSET);
  }

  public void setRightSibling(OBonsaiBucketPointer pointer) throws IOException {
    super.setBucketPointer(offset + RIGHT_SIBLING_OFFSET, pointer);
  }

  /**
   * @return the identifier associated with this bucket, has a meaning only for the root bucket
   */
  public long getIdentifier() {
    return version >= VERSION_2 ? getLongValue(offset + ID_OFFSET) : 0;
  }

  /**
   * Sets the identifier associated with this bucket, has a meaning only for the root bucket.
   *
   * @param value the new identifier value
   */
  public void setIdentifier(long value) throws IOException {
    if (version >= VERSION_2)
      setLongValue(offset + ID_OFFSET, value);
  }

  @Override
  protected void setBucketPointer(int pageOffset, OBonsaiBucketPointer value) throws IOException {
    if (version >= VERSION_2) {
      assert value.getPageOffset() >= 0 && value.getPageOffset() <= 0xFFFF;
      setLongValue(pageOffset, value.getPageIndex());
      setShortValue(pageOffset + OLongSerializer.LONG_SIZE, (short) value.getPageOffset());
    } else
      super.setBucketPointer(pageOffset, value);
  }

  @Override
  protected OBonsaiBucketPointer getBucketPointer(int offset) {
    return version >= VERSION_2 ?
        new OBonsaiBucketPointer(getLongValue(offset), getShortValue(offset + OLongSerializer.LONG_SIZE) & 0xFFFF) :
        super.getBucketPointer(offset);
  }

  private void checkEntreeSize(int entreeSize) {
    if (entreeSize > MAX_ENTREE_SIZE)
      throw new OSBTreeBonsaiLocalException(
          "Serialized key-value pair size bigger than allowed " + entreeSize + " vs " + MAX_ENTREE_SIZE + ".", tree);
  }

  private int getPositionSize() {
    return version >= VERSION_2 ? OShortSerializer.SHORT_SIZE : OIntegerSerializer.INT_SIZE;
  }

  private int getPointerSize() {
    return OLongSerializer.LONG_SIZE + (version >= VERSION_2 ? OShortSerializer.SHORT_SIZE : OIntegerSerializer.INT_SIZE);
  }

  private int getPosition(int index) {
    return version >= VERSION_2 ?
        getShortValue(offset + POSITIONS_ARRAY_OFFSET + index * OShortSerializer.SHORT_SIZE) & 0xFFFF :
        getIntValue(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE);
  }

  private int setPosition(int index, int position) {
    if (version >= VERSION_2) {
      assert position >= 0 && position <= 0xFFFF;
      return setShortValue(offset + POSITIONS_ARRAY_OFFSET + index * OShortSerializer.SHORT_SIZE, (short) position);
    } else
      return setIntValue(offset + POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, position);
  }

  private int readPosition(int offset) {
    return version >= VERSION_2 ? getShortValue(offset) & 0xFFFF : getIntValue(offset);
  }

  private void writePosition(int offset, int position) {
    if (version >= VERSION_2) {
      assert position >= 0 && position <= 0xFFFF;
      setShortValue(offset, (short) position);
    } else
      setIntValue(offset, position);
  }

  @SuppressWarnings("unchecked")
  private static <T> OBinarySerializer<T> upgradeSerializer(OBinarySerializer<T> serializer, byte version) {
    if (serializer == null)
      return null;

    switch (version) {
    case VERSION_1:
      return serializer;
    case VERSION_2:
      switch (serializer.getId()) {
      case OLinkSerializer.ID:
        return (OBinarySerializer<T>) OVarLinkSerializer.INSTANCE;
      case OIntegerSerializer.ID:
        return (OBinarySerializer<T>) OVarUnsignedIntegerSerializer.INSTANCE;
      default:
        return serializer;
      }
    default:
      throw new IllegalStateException("unexpected Bonsai bucket version");
    }
  }

  private static byte decodeVersion(byte flags) {
    return (byte) ((flags & VERSION_MASK) >>> VERSION_SHIFT);
  }

  private static byte encodeVersion(byte flags, byte version) {
    assert version >= 0 && version < 16;
    return (byte) ((version << VERSION_SHIFT & VERSION_MASK) | (flags & ~VERSION_MASK));
  }

  private static boolean decodeFlag(byte flags, byte mask) {
    return (flags & mask) != 0;
  }

  private static byte encodeFlag(byte flags, byte mask, boolean value) {
    return value ? (byte) (flags | mask) : (byte) (flags & ~mask);
  }

}
