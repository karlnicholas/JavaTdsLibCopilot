// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib;

/**
 * TDS Protocol versions.
 */
public enum TdsVersion {
  /**
   * Version 7.1
   */
  V7_1(0x71000001),

  /**
   * Version 7.2
   */
  V7_2(0x72090002),

  /**
   * Version 7.3.A
   */
  V7_3_A(0x730A0003),

  /**
   * Version 7.3.B
   */
  V7_3_B(0x730B0003),

  /**
   * Version 7.4
   */
  V7_4(0x74000004);

  private final int value;

  TdsVersion(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static TdsVersion fromValue(int value) {
    for (TdsVersion v : values()) {
      if (v.getValue() == value) {
        return v;
      }
    }
    return V7_4; // fallback
  }

  // In TdsVersion enum (add these)
  public int getMajor() {
    return switch (this) {
      case V7_1 -> 7;
      case V7_2 -> 7;
      case V7_3_A, V7_3_B -> 7;
      case V7_4 -> 7;
    };
  }

  public int getMinor() {
    return switch (this) {
      case V7_1 -> 1;
      case V7_2 -> 2;
      case V7_3_A -> 3;
      case V7_3_B -> 3;
      case V7_4 -> 4;
    };
  }

  public int getBuild() {
    return switch (this) {
      case V7_1 -> 0; // or actual
      case V7_2 -> 0;
      case V7_3_A -> 0;
      case V7_3_B -> 0;
      case V7_4 -> 0;
    };
  }

  public String toVersionString() {
    return getMajor() + "." + getMinor() + "." + getBuild();
  }
}