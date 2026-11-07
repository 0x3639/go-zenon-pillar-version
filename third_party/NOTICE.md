# Upstream code and notices

Pinned source: zenon-network/go-zenon at
`bb39e31c608dc397a91b98b21b096bca99a9f4d6`.
The root LICENSE reproduces the upstream GPL version 3 license.
Observer additions are distributed under GPL-3.0-or-later.

`internal/transport/rlpx.go` is adapted from upstream `p2p/rlpx.go`.
Its original copyright and LGPL-3.0-or-later notice are preserved.
Changes: package name, concrete adapter constructor return type, a 10 MiB
frame payload bound checked before allocation, the same outbound bound,
timestamp capture immediately after the complete encrypted frame arrives,
and keyed fields in a public-key struct literal for current Go static checks.
The old transport's handshake, key derivation, MAC and encoding are preserved.
`transport.go` adapts the pinned Hello layout and uses upstream message types.
The `protocol` package reproduces the documented eth/61 status wire layout.

The dependency manifest and checksums are in go.mod and go.sum. Build metadata
also embeds the resolved module version. This is not a modern Ethereum RLPx
implementation and must not be substituted without compatibility tests.
`dependencies.json` lists the explicit module requirements and their available
root license notices, copied without edits under `licenses/`.

When distributing binaries, provide this repository's corresponding source,
dependency manifest, license texts and these modification notices.

SQLite access uses github.com/mattn/go-sqlite3 v1.14.53 (MIT); its bundled SQLite
amalgamation identifies itself as version 3.54.1. The driver license is included
under licenses/github.com/mattn/go-sqlite3/v1.14.53/.
