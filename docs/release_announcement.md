# ExArrow 0.1.0 — Release notice for Elixir news outlets

Copy-paste ready text for the Elixir Forum, Elixir Weekly, and similar channels. No edits required except replacing `your-org` with the actual GitHub org/username if you use the GitHub link.

---

**Subject (e.g. for newsletter):** ExArrow 0.1.0 — Apache Arrow for the BEAM (IPC, Flight, ADBC)

**Short (one paragraph):**

ExArrow 0.1.0 is now on Hex. It brings Apache Arrow support to the BEAM: IPC (stream and file read/write), Arrow Flight (client and echo server over gRPC), and ADBC bindings for Arrow-native database connectivity. Data stays in native Rust/Arrow buffers; Elixir uses opaque handles. Precompiled NIFs are included for Linux, macOS, and Windows so no Rust toolchain is required. Elixir ~> 1.18, OTP 26+. [Hex](https://hex.pm/packages/ex_arrow) | [GitHub](https://github.com/your-org/ex_arrow) | [Docs](https://hexdocs.pm/ex_arrow)

---

**Medium (two short paragraphs):**

ExArrow 0.1.0 has been released to Hex. The library provides Apache Arrow support on the BEAM with three components: IPC for reading and writing Arrow streams and files (including random-access file format), Arrow Flight for gRPC-based transfer with a client and built-in echo server, and ADBC for database connectivity that returns Arrow result sets.

Arrow data lives in native (Rust) memory; Elixir holds lightweight handles. Precompiled NIFs are shipped for common platforms so users do not need Rust installed. Requirements: Elixir ~> 1.18, OTP 26+.

- Package: https://hex.pm/packages/ex_arrow  
- Documentation: https://hexdocs.pm/ex_arrow  
- Source: https://github.com/your-org/ex_arrow  

---

**Forum post (slightly longer):**

**ExArrow 0.1.0 — Apache Arrow for the BEAM**

I’ve published ExArrow 0.1.0 to Hex. It adds Apache Arrow support for Elixir/OTP with:

- **IPC**: Stream and file format. Read/write from binary or file; random-access file API (schema, batch count, get batch by index).
- **Arrow Flight**: Client and echo server. do_put/do_get, list_flights, get_flight_info, get_schema, list_actions, do_action. Plaintext HTTP/2 (TLS planned later).
- **ADBC**: Database, Connection, Statement. Open by driver path or name; execute SQL to an Arrow stream. Metadata APIs (get_table_types, get_table_schema, get_objects) and Statement.bind where the driver supports them.

Data stays in native Arrow buffers; the BEAM holds opaque handles. Precompiled Rust NIFs are included for Linux, macOS, and Windows (RustlerPrecompiled), so no Rust is required for normal use.

Elixir ~> 1.18, OTP 26+.

- Hex: https://hex.pm/packages/ex_arrow  
- Docs: https://hexdocs.pm/ex_arrow  
- GitHub: https://github.com/your-org/ex_arrow  

Feedback and issues welcome.
