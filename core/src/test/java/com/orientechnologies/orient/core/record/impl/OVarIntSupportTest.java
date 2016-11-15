package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSupport;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class OVarIntSupportTest {

  @Test
  public void serializeZero() {
    BytesContainer bytes = new BytesContainer();
    OVarIntSupport.write(bytes, 0);
    bytes.offset = 0;
    assertEquals(OVarIntSupport.readAsLong(bytes), 0l);
  }

  @Test
  public void serializeNegative() {
    BytesContainer bytes = new BytesContainer();
    OVarIntSupport.write(bytes, -20432343);
    bytes.offset = 0;
    assertEquals(OVarIntSupport.readAsLong(bytes), -20432343l);
  }

  @Test
  public void serializePositive() {
    BytesContainer bytes = new BytesContainer();
    OVarIntSupport.write(bytes, 20432343);
    bytes.offset = 0;
    assertEquals(OVarIntSupport.readAsLong(bytes), 20432343l);
  }

  @Test
  public void serializeCrazyPositive() {
    BytesContainer bytes = new BytesContainer();
    OVarIntSupport.write(bytes, 16238);
    bytes.offset = 0;
    assertEquals(OVarIntSupport.readAsLong(bytes), 16238l);
  }

  @Test
  public void serializePosition() {
    BytesContainer bytes = new BytesContainer();
    bytes.offset = OVarIntSupport.write(bytes, 16238);
    assertEquals(OVarIntSupport.readAsLong(bytes), 16238l);
  }

  @Test
  public void serializeMaxLong() {
    BytesContainer bytes = new BytesContainer();
    bytes.offset = OVarIntSupport.write(bytes, Long.MAX_VALUE);
    assertEquals(OVarIntSupport.readAsLong(bytes), Long.MAX_VALUE);
  }

  @Test
  public void serializeMinLong() {
    BytesContainer bytes = new BytesContainer();
    bytes.offset = OVarIntSupport.write(bytes, Long.MIN_VALUE);
    assertEquals(OVarIntSupport.readAsLong(bytes), Long.MIN_VALUE);
  }

}
