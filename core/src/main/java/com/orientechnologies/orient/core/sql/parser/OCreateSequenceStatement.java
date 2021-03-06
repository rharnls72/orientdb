/* Generated By:JJTree: Do not edit this line. OCreateSequenceStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import java.util.Map;

public class OCreateSequenceStatement extends OStatement {
  public static final int TYPE_CACHED  = 0;
  public static final int TYPE_ORDERED = 1;

  OIdentifier name;
  int         type;
  OExpression start;
  OExpression increment;
  OExpression cache;

  public OCreateSequenceStatement(int id) {
    super(id);
  }

  public OCreateSequenceStatement(OrientSql p, int id) {
    super(p, id);
  }

  @Override public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("CREATE SEQUENCE ");
    name.toString(params, builder);
    builder.append(" TYPE ");
    switch (type) {
    case TYPE_CACHED:
      builder.append(" CACHED");
      break;
    case TYPE_ORDERED:
      builder.append(" ORDERED");
      break;
    default:
      throw new IllegalStateException("Invalid type for CREATE SEQUENCE: " + type);
    }

    if (start != null) {
      builder.append(" START ");
      start.toString(params, builder);
    }
    if (increment != null) {
      builder.append(" INCREMENT ");
      increment.toString(params, builder);
    }
    if (cache != null) {
      builder.append(" CACHE ");
      cache.toString(params, builder);
    }
  }
}
/* JavaCC - OriginalChecksum=b0436d11e05c3435f22dafea6b5106c0 (do not edit this line) */
