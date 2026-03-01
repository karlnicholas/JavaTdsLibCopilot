package org.tdslib.javatdslib.protocol;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Result;
import java.util.ArrayList;
import java.util.List;

import org.tdslib.javatdslib.api.*;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;
import org.tdslib.javatdslib.tokens.models.ReturnValueToken;
import org.tdslib.javatdslib.transport.ConnectionContext;

public class SegmentTranslator {

  public static Result.Segment createRowSegment(List<byte[]> columnData,
                                                ColMetaDataToken metaDataToken,
                                                ConnectionContext context) {
    List<ColumnMetadata> metaList = new ArrayList<>();
    if (metaDataToken != null) {
      for (ColumnMeta cm : metaDataToken.getColumns()) {
        metaList.add(new TdsColumnMetadata(cm));
      }
    }
    return new TdsRowSegment(new TdsRow(columnData, metaList,
        context.getVarcharCharset()));
  }

  public static Result.OutSegment createOutSegment(List<ReturnValueToken> tokens, ConnectionContext context) {
    List<byte[]> values = new ArrayList<>(tokens.size());
    List<TdsOutParameterMetadata> metaList = new ArrayList<>(tokens.size());

    for (ReturnValueToken token : tokens) {
      values.add((byte[]) token.getValue());
      metaList.add(new TdsOutParameterMetadata(token));
    }

    return new TdsOutSegment(new TdsOutParameters(values, metaList, context.getVarcharCharset()));
  }
}