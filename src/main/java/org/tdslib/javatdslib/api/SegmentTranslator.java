package org.tdslib.javatdslib.api;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Result;
import java.util.ArrayList;
import java.util.List;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;
import org.tdslib.javatdslib.tokens.models.ReturnValueToken;
import org.tdslib.javatdslib.transport.ConnectionContext;

/**
 * A utility class for translating TDS protocol components into R2DBC {@link Result.Segment}s. This
 * class provides static methods to create row segments and output parameter segments from the raw
 * data received from the database, bridging the gap between the TDS protocol and the R2DBC API.
 */
public class SegmentTranslator {

  /**
   * Creates a {@link Result.Segment} representing a row of data.
   *
   * @param columnData The raw byte data for each column in the row.
   * @param metaDataToken The token containing metadata for the columns.
   * @param context The connection context, used to access session-specific settings like charsets.
   * @return A {@link TdsRowSegment} containing the structured row data.
   */
  public static Result.Segment createRowSegment(
      List<Object> columnData, ColMetaDataToken metaDataToken, ConnectionContext context) {
    List<ColumnMetadata> metaList = new ArrayList<>();
    if (metaDataToken != null) {
      for (ColumnMeta cm : metaDataToken.getColumns()) {
        metaList.add(new TdsColumnMetadata(cm));
      }
    }
    return new TdsRowSegment(new TdsRow(columnData, metaList, context.getVarcharCharset()));
  }

  /**
   * Creates a {@link Result.OutSegment} for output parameters returned from a stored procedure.
   *
   * @param tokens A list of {@link ReturnValueToken}s, each representing an output parameter.
   * @param context The connection context for session-specific settings.
   * @return A {@link TdsOutSegment} containing the output parameter data.
   */
  public static Result.OutSegment createOutSegment(
      List<ReturnValueToken> tokens, ConnectionContext context) {
    List<byte[]> values = new ArrayList<>(tokens.size());
    List<TdsOutParameterMetadata> metaList = new ArrayList<>(tokens.size());

    for (ReturnValueToken token : tokens) {
      values.add((byte[]) token.getValue());
      metaList.add(new TdsOutParameterMetadata(token));
    }

    return new TdsOutSegment(
        new TdsOutParameters(values, metaList, context.getVarcharCharset()));
  }
}
