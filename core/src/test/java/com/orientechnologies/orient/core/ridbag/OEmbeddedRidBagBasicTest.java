package com.orientechnologies.orient.core.ridbag;

import static org.testng.AssertJUnit.assertEquals;

import java.util.UUID;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.record.ridbag.embedded.OEmbeddedRidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;

public class OEmbeddedRidBagBasicTest {

  @Test
  public void embeddedRidBagSerializationTest() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + OEmbeddedRidBag.class.getSimpleName());
    db.create();
    try {
      OEmbeddedRidBag bag = new OEmbeddedRidBag();

      bag.add(new ORecordId(3, 1000));
      bag.convertLinks2Records();
      bag.convertRecords2Links();
      BytesContainer container = new BytesContainer();
      UUID id = UUID.randomUUID();
      bag.serialize(container, id, ORidBag.Encoding.Original);

      container.offset = 0;
      OEmbeddedRidBag bag1 = new OEmbeddedRidBag();
      bag1.deserialize(container, ORidBag.Encoding.Original);

      assertEquals(bag.size(), 1);

      assertEquals(null, bag1.iterator().next());
    } finally {
      db.drop();
    }

  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testExceptionInCaseOfNull() {
    OEmbeddedRidBag bag = new OEmbeddedRidBag();
    bag.add(null);

  }

}
