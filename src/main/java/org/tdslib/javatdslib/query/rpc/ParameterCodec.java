package org.tdslib.javatdslib.query.rpc;

import java.nio.ByteBuffer;

/**
 * Defines the contract for serializing a specific data type into the MS-TDS RPC stream.
 */
public interface ParameterCodec {

  /**
   * Evaluates whether this codec can handle the provided parameter.
   */
  boolean canEncode(ParamEntry entry);

  /**
   * Generates the SQL declaration string for this parameter (e.g., "nvarchar(4000)").
   */
  String getSqlTypeDeclaration(ParamEntry entry);

  /**
   * Writes the TDS TYPE_INFO metadata block (type byte, length, precision, scale, collation).
   */
  void writeTypeInfo(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context);

  /**
   * Writes the actual binary payload of the value.
   */
  void writeValue(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context);
}