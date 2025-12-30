# JavaTdsLibCopilot

This repository contains a Java implementation of the TDS (Tabular Data Stream) protocol, converted from the C# TdsLib library (https://github.com/microsoft/TdsLib).

## Overview

The TDS protocol is used for communication between SQL Server clients and servers. This Java library provides an open implementation focused on login steps and generic TDS features.

## Project Structure

- `src/main/java/com/microsoft/data/tools/tdslib/` - Main source code
  - `buffer/` - Buffer classes for type conversion
  - `io/` - Connection-related classes
  - `messages/` - Logical message formats
  - `packets/` - Packet handling classes
  - `payloads/` - Message payloads
  - `tokens/` - Token classes for server responses

## Building

This is a Maven project. To build:

```bash
mvn compile
```

## Status

This is an ongoing conversion from C# to Java. Currently, basic constants, enums, and token classes have been converted. The full conversion is in progress.

## Original C# Library

The original C# implementation can be found at: https://github.com/microsoft/TdsLib