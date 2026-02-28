package org.tdslib.javatdslib;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Result;
import java.util.ArrayList;
import java.util.List;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;
import org.tdslib.javatdslib.tokens.returnvalue.ReturnValueToken;
import org.tdslib.javatdslib.tokens.row.RowToken;
import org.tdslib.javatdslib.transport.ConnectionContext;

/**
 * Translates TDS tokens into R2DBC Result Segments.
 */
public class SegmentTranslator {

  /**
   * Creates a RowSegment from a RowToken.
   *
   * @param token   the row token
   * @param ctx     the query context
   * @param context the connection context
   * @return a new RowSegment
   */
  public static Result.Segment createRowSegment(RowToken token, QueryContext ctx,
                                                ConnectionContext context) {
    List<ColumnMetadata> metaList = new ArrayList<>();
    if (ctx.getColMetaDataToken() != null) {
      for (ColumnMeta cm : ctx.getColMetaDataToken().getColumns()) {
        metaList.add(new TdsColumnMetadata(cm));
      }
    }
    return new TdsRowSegment(new TdsRow(token.getColumnData(), metaList,
        context.getVarcharCharset()));
  }

  /**
   * Creates an OutSegment from collected return values.
   *
   * @param ctx     the query context
   * @param context the connection context
   * @return a new OutSegment
   */
  public static Result.OutSegment createOutSegment(QueryContext ctx, ConnectionContext context) {
    List<ReturnValueToken> tokens = ctx.getReturnValues();
    List<byte[]> values = new ArrayList<>(tokens.size());
    List<TdsOutParameterMetadata> metaList = new ArrayList<>(tokens.size());

    for (ReturnValueToken token : tokens) {
      values.add((byte[]) token.getValue());
      metaList.add(new TdsOutParameterMetadata(token));
    }

    return new TdsOutSegment(new TdsOutParameters(values, metaList, context.getVarcharCharset()));
  }
}