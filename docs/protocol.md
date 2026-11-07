# Protocol adapter and interpretation

The transport adapter is based on `p2p/rlpx.go` from the pinned go-zenon
revision, with the original LGPL notice preserved. The code's legacy MAC and
encrypted auth exchange must not be replaced by modern Ethereum assumptions.
The adapter has no discovery service, inbound listener, full protocol manager,
transaction propagation or chain synchronizer.

The only advertised capability is `eth/61`. Its nine subprotocol message codes
start at offset 16. The status fields are protocol version, network ID, momentum
height (upstream `TD`), frontier hash and genesis hash. Mainnet network/chain ID
is 1 and the genesis hash is
`9e204601d1b7b1427fe12bc82622e610d8a6ad43c40abf020eb66e538bb8eeb0`.
The genesis constant comes from the pinned embedded genesis checksum test.

Before a new connection, an actual recent header is fetched from one of the
configured public WSS reference endpoints and checked. Its hash, height,
producer, source and verification state are recorded in the session. A stale
reference prevents new dials; it does not invent a frontier. RPC reads have an
eight-second deadline and a 256 KiB response cap. One worker serializes
reference calls and hash resolution; frontier refresh targets ten seconds but
can be delayed by resolution or source failure. This limitation is visible in
reference freshness. Multi-source agreement and chain continuity are milestone 2.

The RLPx reader rejects an oversized frame **before** payload allocation.
The maximum payload is 10 MiB, with one byte for the wire code. Complete-frame
time is captured before decryption and RLP decoding, after frame authentication.
Thus the timestamp includes network, framing and authentication work at this
observer. It is not a network-wide propagation time.

Decoded RLP is bounded to 4,096 elements and 16 nesting levels. Hash lists are
limited to 256 hashes, momentum content and account-body lists to 100 elements,
and block-response batches to 16. Account bodies are left raw and discarded.
The experiment has no payload cache and serves protocol-valid empty lists for
history and detailed-block requests. Headers do not imply that their transaction
bodies are available. Transaction messages are discarded within the frame cap.

For each full momentum the observer checks structure, chain ID 1, version 1,
the recomputed hash, and standard Ed25519 verification of `Hash.Bytes()` as used
by the pinned wallet verifier. The producer address is derived from the public
key with upstream `types.PubKeyToAddress`. This checks the signing key; it does
not validate account transitions, pillar registration, slot eligibility or
consensus. RPC records receive the same cryptographic checks before attribution.

An announcement's arrival is retained immediately. A matching checked payload
or RPC record resolves its producer asynchronously. Unresolved announcements
have no producer. Requests are limited to one announced hash per peer per ten
seconds, one outstanding request per session, with a 30-second match lifetime.
Responses on the block-response message code count as `requested_response`
only when matched; other responses are `historical_or_unresolved`. Neither
category contributes to unsolicited live counts.

Events are keyed by boot, session, hash and kind. Duplicate counters saturate at
65,535 and are included in final summaries. Each session stores its own IP,
raw advertised client name, parsed version (or null), capabilities and status
reference. A later session cannot rewrite earlier versions. No identity or
ownership assertion is created from timing, and producer software remains null.

Clock comparison uses elapsed UTC versus elapsed monotonic time every ten
seconds. A discontinuity over 100 ms is recorded. For this short feasibility
run, any clock discontinuity or capture gap prevents a passing result for that
boot. There are no rankings to resume; later ranking recovery needs milestone 2
window invalidation. A metadata queue overflow, full arrival map or failed
writer is visible, never silently counted as clean coverage.

The experiment intentionally omits pillar-name lookup, disputed-chain states,
reorganization recovery, association denominators and daily/holdout analysis.
Those outputs must not be inferred from the diagnostic live counts.
