# Babel Plumtree (Epidemic Broadcast Trees)

Push-lazy-push multicast tree dissemination for [Babel](https://github.com/)
applications. Provided and evolved independently by ParadigmShift.

**Group ID:** `pt.paradigmshift.babel`
**Artifact ID:** `plumtree`
**Current version:** `0.1.0`
**Tested with:** `pt.paradigmshift.babel:babel-core` (Babel-Swarm core fork) and
`pt.paradigmshift.babel:babel-protocols-common` (shared dissemination /
membership API surface).
**Source / target:** Java 17.

This module ships **two** dissemination protocols built on the same wire
messages:

| Protocol | Slot | Tree model |
|---|---|---|
| `Plumtree` | `3000` | **single shared tree** — one spanning tree, shared by all senders |
| `MultiPlumtree` | `3100` | **multiple trees** — a separate spanning tree per source (root) |

## Academic reference

This is an independent ParadigmShift implementation of the protocol introduced
in — and any work building on it is asked to cite:

> João Leitão, José Pereira, and Luís Rodrigues. **Epidemic Broadcast Trees.**
> In *Proc. 26th IEEE International Symposium on Reliable Distributed Systems
> (SRDS 2007)*, pp. 301–310, Beijing, China, October 2007. IEEE.
> doi:[10.1109/SRDS.2007.27](https://doi.org/10.1109/SRDS.2007.27).
>
> João Leitão. **Gossip-Based Broadcast Protocols.** MSc thesis, Departamento
> de Informática, Faculdade de Ciências da Universidade de Lisboa, 2007.

The handler structure, variable names (`eagerPushPeers`, `lazyPushPeers`,
`missing`, `round`, …) and the optional tree optimization follow Algorithms 1–4
of the SRDS paper. The `MultiPlumtree` variant realizes §3.7 ("Sender-Based vs
Shared Trees"): rather than running a separate protocol instance per sender, it
keeps **per-root** eager/lazy peer sets inside a single instance, so every
source converges to its own latency-optimal tree.

## Implementation acknowledgement

`MultiPlumtree` goes beyond the paper in *how* it does lazy push. The paper's
lazy push is a one-shot per-message timer; `MultiPlumtree` instead uses
**per-root trees**, a **periodic lazy tick** over a standing queue of
outstanding announcements, and an **`IGNORED_IHAVE`** acknowledgement that
retires an announcement once a peer confirms it already has the message. That
design comes from Riak's `riak_core_broadcast` module, **originally implemented
by Jordan West**. No code from that project is reused here — the acknowledgement
is for the design. With thanks to Jordan West and the riak_core authors.

- `riak_core_broadcast`: <https://github.com/basho/riak_core/blob/develop/src/riak_core_broadcast.erl>

## What it does

A node broadcasting a message delivers it locally, **eager-pushes** the full
payload to its eager peers (the branches of an embedded spanning tree), and
**lazy-pushes** only the message identifier (`IHAVE`) to the rest of the
overlay.

- A peer receiving a **novel** payload delivers it, re-pushes it eagerly and
  lazily, and grafts the sender into its eager set (the link becomes a tree
  branch).
- A peer receiving a **duplicate** payload prunes the sender into its lazy set
  and replies with a `PRUNE`, so the redundant branch is removed at both ends.
- A peer that gets an `IHAVE` for a message it has not seen waits for the
  payload; if it does not arrive in time, it `GRAFT`s the announcer — which
  both retransmits the missing payload **and** repairs the (failed) tree.

Eager push gives low latency; lazy push provides the reliability of pure gossip
and heals the tree after failures — at far lower steady-state overhead than
flooding, because most links carry only identifiers.

The two protocols differ only in how the eager/lazy partition is keyed and how
lazy push is scheduled:

- **`Plumtree` (single shared tree).** One global `eagerPushPeers` /
  `lazyPushPeers` partition. Lazy announcements use a one-shot dispatch policy
  (immediate, or batched on a period). Recovery uses a per-message timer with
  two timeouts: a long `timeout1` wait for the payload, then a shorter
  `timeout2` re-arm that tries the next announcer. An optional **round-based
  optimization** (Algorithm 4) swaps a long eager link for a shorter lazy one
  when their hop-count difference exceeds a threshold.
- **`MultiPlumtree` (multiple trees).** `eagerSets` / `lazySets` keyed by the
  message's originating **root**, so each source has its own tree. Lazy push is
  queued per peer and flushed on a periodic **lazy tick**; an announcement is
  re-sent every tick until the peer either grafts it (was missing) or
  acknowledges it (`IGNORED_IHAVE`, already had it). This standing
  re-advertisement is what makes lazy push reliable here, in place of the
  single-tree per-message timer.

Both protocols have a recovery mechanism of their own (graft-on-missing), so
neither needs to be paired with an external anti-entropy protocol.

## Protocol & event identifiers

Follows the Babel ID convention used across the ParadigmShift workspace:
protocol IDs at 100-multiples; events numbered per handler class from
`protocol_id + 1` upward, in four independent pools (notifications, messages,
requests/replies, timers). Slots `3000` / `3100` sit after the `2600`–`2900`
block reserved for `stoneflux-edgegateway`.

**Messages (shared by both protocols).** Because a deployment runs *one* of the
two protocols — never both at once — the wire messages are a single shared set,
numbered in the `Plumtree` (slot 3000) message pool. `MultiPlumtree`
deliberately reuses them rather than duplicating the pool at `3101+`.

| Type | Handler class | ID | Used by | Purpose |
|---|---|---|---|---|
| `GossipMessage` | message | `3001` | both | Eager-push payload (UUID, round, original sender = root, payload) |
| `PruneMessage` | message | `3002` | both | Demote sender to lazy (carries root in `MultiPlumtree`) |
| `GraftMessage` | message | `3003` | both | Re-attach link + request retransmission (nullable mid; root in `MultiPlumtree`) |
| `IHaveMessage` | message | `3004` | both | Batched lazy announcements `(mid, round)` (root in `MultiPlumtree`) |
| `IgnoredIHaveMessage` | message | `3005` | `MultiPlumtree` | Acknowledge already-held announcements so re-advertisement stops |

**Timers.**

| Type | Handler class | ID | Owner | Purpose |
|---|---|---|---|---|
| `IHaveTimeout` | timer | `3001` | `Plumtree` | Per-message recovery timer (carries the awaited mid) |
| `DispatchTimer` | timer | `3002` | `Plumtree` | Periodic lazy-queue flush (when batching is enabled) |
| `LazyTickTimer` | timer | `3101` | `MultiPlumtree` | Periodic lazy-queue flush + re-advertisement |

**Common API surface consumed / emitted** (from `babel-protocols-common`):

- Consumes `BroadcastRequest` (`501`); emits `BroadcastDelivery` (`501`).
- Consumes `NeighborUp` (`401`) / `NeighborDown` (`402`) from any membership
  protocol (typically HyParView).
- Consumes `ChannelAvailableNotification` (`001`) in shared-channel mode; emits
  it in own-channel mode.

## Channel modes

Both protocols support two channel modes, selected by the
`*.UseSharedChannel` parameter:

- **Own channel** (default). The protocol creates and binds its own
  `TCPChannel`, opens one connection per up-neighbour, manages reconnection,
  and announces the channel via `ChannelAvailableNotification` so other
  protocols may share it. Channels announced by *other* protocols are ignored.
- **Shared channel** (`UseSharedChannel=true`). The protocol opens no channel;
  it waits for a `ChannelAvailableNotification` (e.g. from HyParView) and
  attaches to that channel with `registerSharedChannel`. Connection management
  is then the owner's responsibility — `NeighborUp`/`NeighborDown` only update
  the tree sets; no connections are opened or closed.

In own-channel mode every node is assumed to run the protocol on the same port
(`*.Channel.Port`); set `*.LocalSupport=true` for single-host test deployments,
where a neighbour's port is taken as its membership port `+ 1`.

## Configuration

### `Plumtree` (slot 3000)

| Property | Default | Description |
|---|---|---|
| `Plumtree.UseSharedChannel` | `false` | Attach to a channel announced by another protocol instead of opening one. |
| `Plumtree.Channel.Address` | from `myself` | TCP bind address (own-channel mode). |
| `Plumtree.Channel.Port` | from `myself` | TCP bind port (own-channel mode). |
| `Plumtree.Timeout1` | `1000` ms | Long recovery timeout: wait after the first `IHAVE` before grafting. Set from overlay diameter × target recovery latency. |
| `Plumtree.Timeout2` | `500` ms | Short re-arm after a graft (≈ one neighbour RTT); must be `< Timeout1`. |
| `Plumtree.DispatchPeriod` | `0` ms | `0` = send each `IHAVE` immediately; `> 0` = batch and flush on this period. |
| `Plumtree.Optimization` | `false` | Enable the round-based tree optimization (Algorithm 4). |
| `Plumtree.Optimization.Threshold` | `3` | Min. eager-vs-lazy hop-count difference before swapping links. Lower = more aggressive / less stable. |
| `Plumtree.DeliveredTimeout` | `600000` ms | How long a delivered message is kept (dedup + graft replay window). |
| `Plumtree.LocalSupport` | `false` | Single-host tests: neighbour port = membership port `+ 1`. |

### `MultiPlumtree` (slot 3100)

| Property | Default | Description |
|---|---|---|
| `MultiPlumtree.UseSharedChannel` | `false` | Attach to a channel announced by another protocol instead of opening one. |
| `MultiPlumtree.Channel.Address` | from `myself` | TCP bind address (own-channel mode). |
| `MultiPlumtree.Channel.Port` | from `myself` | TCP bind port (own-channel mode). |
| `MultiPlumtree.LazyTickPeriod` | `1000` ms | Lazy-queue flush / re-advertisement period. Bounds tree-repair latency. |
| `MultiPlumtree.DeliveredTimeout` | `600000` ms | How long a delivered message is kept (dedup + graft replay window). |
| `MultiPlumtree.LocalSupport` | `false` | Single-host tests: neighbour port = membership port `+ 1`. |

## How application protocols plug in

Issue a `BroadcastRequest(sender, payload, protoID)`; receive a
`BroadcastDelivery(sender, payload, timestamp)` notification on every node when
the message arrives. The protocol is oblivious to the payload shape — it
carries opaque bytes.

Membership comes from any protocol that fires `NeighborUp` / `NeighborDown`
— typically `babel-hyparview` (which also announces a shareable channel), or a
static-peer-list wrapper.

### Example (shared HyParView channel)

```properties
# membership owns the channel
babel.discovery pt.unl.fct.di.novasys.babel.core.protocols.discovery.MulticastDiscoveryProtocol
HyParView.Channel.Port 5000

# dissemination shares it
Plumtree.UseSharedChannel true
```

## Build

```bash
mvn clean install
```

Depends on `pt.paradigmshift.babel:babel-core` and
`pt.paradigmshift.babel:babel-protocols-common` from the ParadigmShift Maven
repository (`https://maven.paradigmshift.pt/releases`), listed in `pom.xml`.

## Tuning notes

- **`Plumtree` vs `MultiPlumtree`.** A single shared tree is the cheapest in
  memory and signalling and is ideal when one (or few) nodes broadcast. When
  many distinct sources broadcast and latency matters, `MultiPlumtree` gives
  each source its own optimal tree at the cost of per-root state.
- **Fanout = active-view size.** Plumtree uses *all* overlay neighbours (eager
  + lazy), so the effective fanout is the membership active-view size. Tune
  reliability/overhead via the membership protocol's active-view size, not a
  fanout knob.
- **`Timeout1`** trades recovery latency against redundant grafts: too small and
  healthy-but-slow eager deliveries get grafted needlessly; too large and a real
  loss takes longer to repair. Start near the overlay's expected delivery time.
- **`MultiPlumtree.LazyTickPeriod`** is the analogous knob: it bounds repair
  latency *and* sets how often unacknowledged announcements are retried. Lower
  it for faster healing, raise it to cut control traffic.
- **`DeliveredTimeout`** must comfortably exceed the time a gossip wave (plus
  any recovery round-trips) takes to reach every node, or a graft can arrive
  after the payload has aged out of the cache and cannot be served.
