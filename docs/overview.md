# Overview

ExArrow is an Elixir library that provides Apache Arrow support on the BEAM: IPC (stream and file), Arrow Flight (client and server), and ADBC (Arrow Database Connectivity) bindings with an ergonomic Elixir API.

## What ExArrow is

- **Native core**: Arrow data lives in Rust/Arrow buffers. Elixir holds lightweight handles (resources). This avoids large BEAM heap copies and keeps interoperability efficient.
- **Stable core API**: Schema, Field, Array, RecordBatch, Table, Stream as opaque references. Small metadata is returned as Elixir structs; bulk data stays native until you explicitly copy.
- **Three pillars**: IPC for reading/writing Arrow streams and files; Flight for gRPC-based transfer and services; ADBC for database connectivity with Arrow result sets.

## What ExArrow is not

- Not a full in-memory dataframe library (like Polars). It focuses on interchange and streaming.
- Not a replacement for Ecto or database drivers; ADBC is for Arrow-native DB APIs where available.

## Status

Milestone 0 provides the skeleton: module layout, public API outline, resource-handle strategy, a minimal NIF that loads, tests, and docs. IPC, Flight, and ADBC implementations follow in later milestones.
