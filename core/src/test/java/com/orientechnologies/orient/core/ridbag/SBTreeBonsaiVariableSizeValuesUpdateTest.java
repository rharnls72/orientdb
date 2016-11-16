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

package com.orientechnologies.orient.core.ridbag;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsaiBucket;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Sergey Sitnikov
 */
public class SBTreeBonsaiVariableSizeValuesUpdateTest {

  private ODatabaseDocumentTx                   db;
  private OSBTreeBonsai<OIdentifiable, Integer> tree;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + SBTreeBonsaiVariableSizeValuesUpdateTest.class.getName());
    db.create();
    tree = db.getSbTreeCollectionManager().createAndLoadTree(5);
  }

  @Test
  public void test() {
    for (int r = 0; r < OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES / 4 /* guarantees split */; ++r) {
      final ORecordId rid = new ORecordId(5, r);
      for (int i = 0; i >= 0 && i < Integer.MAX_VALUE; i += Integer.MAX_VALUE / 10) {
        tree.put(rid, r + i);
        Assert.assertEquals(r + i, (int) tree.get(rid));
      }
    }
  }

  @After
  public void after() {
    db.getSbTreeCollectionManager().releaseSBTree(tree.getCollectionPointer());
    db.drop();
  }

}
