package com.orientechnologies.orient.core.record.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;

public class BytesContainerTest {

  @Test
  public void testSimple() {
    BytesContainer bytesContainer = new BytesContainer();
    assertNotNull(bytesContainer.bytes);
    assertEquals(bytesContainer.offset, 0);
  }

  @Test
  public void testReallocSimple() {
    BytesContainer bytesContainer = new BytesContainer();
    bytesContainer.alloc((short) 2050);
    assertTrue(bytesContainer.bytes.length > 2050);
    assertEquals(bytesContainer.offset, 2050);
  }

  @Test
  public void testBorderReallocSimple() {
    BytesContainer bytesContainer = new BytesContainer();
    bytesContainer.alloc((short) 1024);
    int pos = bytesContainer.alloc((short) 1);
    bytesContainer.bytes[pos] = 0;
    assertTrue(bytesContainer.bytes.length >= 1025);
    assertEquals(bytesContainer.offset, 1025);
  }

  @Test
  public void testReadSimple() {
    BytesContainer bytesContainer = new BytesContainer();
    bytesContainer.skip((short) 100);
    assertEquals(bytesContainer.offset, 100);
  }

  @Test
  public void testAppend() {
    BytesContainer bytesContainer = new BytesContainer();
    bytesContainer.alloc(1);
    BytesContainer bytes1 = new BytesContainer();
    bytes1.alloc(5);
    for (int i = 0; i < 5; i++) {
      bytes1.bytes[i] = (byte) (i + 1);
    }
    bytesContainer.append(bytes1);
    assertEquals(bytesContainer.offset, 6);
    assertEquals(bytesContainer.bytes[1], 1);
    assertEquals(bytesContainer.bytes[2], 2);
    assertEquals(bytesContainer.bytes[3], 3);
    assertEquals(bytesContainer.bytes[4], 4);
    assertEquals(bytesContainer.bytes[5], 5);
  }

}
