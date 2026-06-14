package pt.paradigmshift.babel.plumtree;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pt.paradigmshift.babel.plumtree.messages.Announcement;
import pt.paradigmshift.babel.plumtree.messages.GossipMessage;
import pt.paradigmshift.babel.plumtree.messages.GraftMessage;
import pt.paradigmshift.babel.plumtree.messages.IHaveMessage;
import pt.paradigmshift.babel.plumtree.messages.PruneMessage;
import pt.paradigmshift.babel.plumtree.timers.DispatchTimer;
import pt.paradigmshift.babel.plumtree.timers.IHaveTimeout;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.metrics.Counter;
import pt.unl.fct.di.novasys.babel.metrics.Metric;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.notifications.BroadcastDelivery;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.requests.BroadcastRequest;
import pt.unl.fct.di.novasys.babel.protocols.general.notifications.ChannelAvailableNotification;
import pt.unl.fct.di.novasys.babel.protocols.membership.notifications.NeighborDown;
import pt.unl.fct.di.novasys.babel.protocols.membership.notifications.NeighborUp;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * Plumtree — the push-lazy-push multicast tree broadcast protocol.
 *
 * <p>An independent ParadigmShift implementation of the protocol described in:
 * <blockquote>
 * João Leitão, José Pereira, and Luís Rodrigues. <i>Epidemic Broadcast
 * Trees</i>. In Proc. 26th IEEE Symposium on Reliable Distributed Systems
 * (SRDS 2007), pp. 301–310, 2007. doi:10.1109/SRDS.2007.27.
 * </blockquote>
 * and in João Leitão's MSc thesis, <i>Gossip-Based Broadcast Protocols</i>
 * (FCUL, 2007). The variable names below (<i>eagerPushPeers</i>,
 * <i>lazyPushPeers</i>, <i>missing</i>, <i>round</i>, ...) follow the paper's
 * Algorithms 1–4.
 *
 * <h2>How it works</h2>
 * The protocol maintains, over the membership reported by a peer-sampling
 * service (e.g. HyParView), two disjoint peer sets: {@code eagerPushPeers} (the
 * branches of an embedded spanning tree, which carry full payloads) and
 * {@code lazyPushPeers} (the remaining overlay links, which carry only
 * {@code IHAVE} announcements). A node receiving a <b>novel</b> {@link
 * GossipMessage} delivers it, forwards it eagerly, queues {@code IHAVE}s
 * lazily, and grafts the sender into its eager set. A node receiving a
 * <b>duplicate</b> prunes the sender to its lazy set (and sends it a {@link
 * PruneMessage}). Lost messages — and broken tree branches — are recovered via
 * the lazy path: an {@code IHAVE} for an unseen message arms a timer; if the
 * payload does not arrive first, the node {@link GraftMessage grafts} the
 * announcer, which both repairs the tree and retransmits the payload.
 *
 * <p>This is a <b>single shared tree</b>: it is optimized for the first
 * broadcaster and shared by all senders. For per-sender trees see
 * {@link MultiPlumtree}.
 *
 * <h2>Channel modes ({@code Plumtree.PeerAddressResolution})</h2>
 * One parameter selects how a peer's Plumtree endpoint is resolved from the
 * membership endpoint, and whether this protocol owns a channel:
 * <ul>
 *   <li><b>{@code offset}</b> (default): own channel; a peer's port is its
 *       membership port plus {@link #PAR_PORT_OFFSET} (default {@code 1}). The
 *       general case — works whenever nodes may bind different ports.</li>
 *   <li><b>{@code fixed}</b>: own channel; every peer is assumed to listen on
 *       {@link #PAR_PEER_PORT} (defaults to this node's own channel port). For
 *       uniform deployments where all nodes share one port.</li>
 *   <li><b>{@code shared}</b>: opens no channel of its own; it waits for a
 *       {@link ChannelAvailableNotification} (e.g. from HyParView) and attaches
 *       to that channel. Connection management is then the channel owner's
 *       responsibility — neighbour up/down only updates the tree sets.</li>
 * </ul>
 * In the own-channel modes ({@code offset}/{@code fixed}) the protocol binds its
 * own {@link TCPChannel}, opens one connection per up-neighbour, and announces
 * the channel via {@link ChannelAvailableNotification} so others may share it.
 * This mirrors {@code babel-eager-gossip-broadcast}'s {@code PeerAddressResolution}.
 *
 * @see MultiPlumtree
 * @see GossipMessage
 */
public class Plumtree extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(Plumtree.class);

    /** Babel protocol numeric identifier. */
    public static final short PROTOCOL_ID = 3000;
    /** Babel protocol name. */
    public static final String PROTOCOL_NAME = "Plumtree";

    /**
     * Property — how a peer's Plumtree endpoint is resolved from the membership
     * endpoint (and whether this protocol owns a channel). One of
     * {@link #RESOLUTION_OFFSET}, {@link #RESOLUTION_FIXED} or
     * {@link #RESOLUTION_SHARED}. Mirrors {@code babel-eager-gossip-broadcast}.
     */
    public static final String PAR_PEER_ADDRESS_RESOLUTION = "Plumtree.PeerAddressResolution";
    /** {@link #PAR_PEER_ADDRESS_RESOLUTION} value: own channel; peer port = its membership port + {@link #PAR_PORT_OFFSET}. */
    public static final String RESOLUTION_OFFSET = "offset";
    /** {@link #PAR_PEER_ADDRESS_RESOLUTION} value: own channel; every peer listens on {@link #PAR_PEER_PORT}. */
    public static final String RESOLUTION_FIXED = "fixed";
    /** {@link #PAR_PEER_ADDRESS_RESOLUTION} value: attach to a channel announced by another protocol (e.g. HyParView). */
    public static final String RESOLUTION_SHARED = "shared";
    /** Default for {@link #PAR_PEER_ADDRESS_RESOLUTION}: {@value}. */
    public static final String DEFAULT_PEER_ADDRESS_RESOLUTION = RESOLUTION_OFFSET;

    /** Property — port distance from a peer's membership channel to its Plumtree channel ({@code offset} mode only). */
    public static final String PAR_PORT_OFFSET = "Plumtree.PortOffset";
    /** Default for {@link #PAR_PORT_OFFSET}: {@value}. */
    public static final String DEFAULT_PORT_OFFSET = "1";

    /** Property — the uniform port every peer's Plumtree channel listens on ({@code fixed} mode only); defaults to this node's own {@link #PAR_CHANNEL_PORT}. */
    public static final String PAR_PEER_PORT = "Plumtree.PeerPort";

    /** Property — TCP bind address (own-channel mode). Defaults to the {@code myself} host. */
    public static final String PAR_CHANNEL_ADDRESS = "Plumtree.Channel.Address";
    /** Property — TCP bind port (own-channel mode). Defaults to the {@code myself} host. */
    public static final String PAR_CHANNEL_PORT = "Plumtree.Channel.Port";

    /**
     * Property — first (long) recovery timeout in ms ({@code timeout1}): how
     * long a node waits, after the first {@code IHAVE} for an unseen message,
     * for that message to arrive by eager push before grafting. Should be set
     * from the overlay diameter and the target recovery latency.
     */
    public static final String PAR_TIMEOUT1 = "Plumtree.Timeout1";
    /** Default {@code timeout1}: {@value} ms. */
    public static final String DEFAULT_TIMEOUT1 = "1000";

    /**
     * Property — second (short) recovery timeout in ms ({@code timeout2}): the
     * re-arm delay after a graft, in case the grafted neighbour also fails to
     * deliver. Should be smaller than {@code timeout1}, on the order of one
     * round-trip to a neighbour.
     */
    public static final String PAR_TIMEOUT2 = "Plumtree.Timeout2";
    /** Default {@code timeout2}: {@value} ms. */
    public static final String DEFAULT_TIMEOUT2 = "500";

    /**
     * Property — lazy-push dispatch period in ms. When {@code > 0}, {@code IHAVE}
     * announcements are batched and flushed on this period (the paper's
     * scheduling policy). When {@code 0} (default) each announcement is sent
     * immediately.
     */
    public static final String PAR_DISPATCH_PERIOD = "Plumtree.DispatchPeriod";
    /** Default dispatch period: {@value} ms (immediate). */
    public static final String DEFAULT_DISPATCH_PERIOD = "0";

    /** Property — enable the round-based tree optimization (paper Algorithm 4). */
    public static final String PAR_OPTIMIZATION = "Plumtree.Optimization";
    /** Default for {@link #PAR_OPTIMIZATION}: {@value}. */
    public static final boolean DEFAULT_OPTIMIZATION = false;

    /**
     * Property — optimization threshold: the minimum eager-vs-lazy hop-count
     * difference at or above which the optimization swaps a long eager link for
     * a shorter lazy one (a smaller difference leaves the tree unchanged). Lower
     * values optimize more aggressively (less stable); the paper suggests a value
     * close to the overlay diameter for the shared-tree / multi-sender case.
     */
    public static final String PAR_THRESHOLD = "Plumtree.Optimization.Threshold";
    /** Default optimization threshold: {@value} rounds. */
    public static final String DEFAULT_THRESHOLD = "3";

    /** Property — how long a delivered message is remembered (dedup + graft-replay window). */
    public static final String PAR_DELIVERY_TIMEOUT = "Plumtree.DeliveredTimeout";
    /** Default delivery-window timeout: {@value} ms (10 minutes). */
    public static final String DEFAULT_DELIVERY_TIMEOUT = "600000";

    private final long timeout1;
    private final long timeout2;
    private final long dispatchPeriod;
    private final boolean optimization;
    private final int threshold;
    private final long removeTimeWindow;

    /** Peer-address resolution strategy — see {@link #PAR_PEER_ADDRESS_RESOLUTION}. */
    private enum Resolution { OFFSET, FIXED, SHARED }
    private final Resolution resolution;
    private final int portOffset;
    private final int peerPort;

    private final boolean managingChannel;
    private boolean channelReady;
    private int channelId;
    private int networkPort;
    private Host myself;

    /** A queued lazy-push announcement still waiting in {@code missing} for its payload. */
    private record Announce(Host sender, int round) { }

    /** Tree branches: peers we eager-push full payloads to. */
    private final Set<Host> eagerPushPeers = new HashSet<>();
    /** Non-tree links: peers we only send {@code IHAVE} announcements to. */
    private final Set<Host> lazyPushPeers = new HashSet<>();
    /** Membership-level neighbour set (from NeighborUp/Down), in Plumtree-endpoint space. */
    private final Set<Host> neighbors = new HashSet<>();
    /** Own-channel mode: neighbours we are connecting to but have not confirmed yet. */
    private final Set<Host> pending = new HashSet<>();

    /** FIFO of delivered message ids, oldest first — drives the delivery-window GC. */
    private final LinkedList<UUID> receivedInOrder = new LinkedList<>();
    /** Delivered message id → local delivery time (dedup + GC). */
    private final Map<UUID, Long> receivedTimestamps = new HashMap<>();
    /** Delivered message id → full message, retained within the window to answer grafts. */
    private final Map<UUID, GossipMessage> stored = new HashMap<>();

    /** Announced-but-not-yet-received messages → the announcers (insertion order). */
    private final Map<UUID, Deque<Announce>> missing = new HashMap<>();
    /** Message id → the Babel id of its recovery timer (for cancellation). */
    private final Map<UUID, Long> timers = new HashMap<>();
    /** Per-target lazy queue of pending {@code IHAVE} announcements, flushed by dispatch. */
    private final Map<Host, List<Announcement>> lazyQueue = new HashMap<>();

    private final Counter sentMessagesCounter;

    /**
     * Construct the protocol and wire its handlers. Depending on
     * {@link #PAR_PEER_ADDRESS_RESOLUTION} it either binds its own TCP channel
     * now ({@code offset}/{@code fixed}) or subscribes to {@link
     * ChannelAvailableNotification} to attach to a shared one ({@code shared}).
     *
     * @param properties protocol configuration; see the {@code PAR_*} constants
     * @param myself     this node's {@link Host} identity. May be {@code null}
     *                   in shared-channel mode (the channel endpoint is adopted
     *                   when it becomes available); required otherwise unless
     *                   {@code Channel.Address} + {@code Channel.Port} are set.
     */
    public Plumtree(Properties properties, Host myself)
            throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;

        this.timeout1 = Long.parseLong(properties.getProperty(PAR_TIMEOUT1, DEFAULT_TIMEOUT1));
        this.timeout2 = Long.parseLong(properties.getProperty(PAR_TIMEOUT2, DEFAULT_TIMEOUT2));
        this.dispatchPeriod = Long.parseLong(properties.getProperty(PAR_DISPATCH_PERIOD, DEFAULT_DISPATCH_PERIOD));
        this.optimization = readBool(properties, PAR_OPTIMIZATION, DEFAULT_OPTIMIZATION);
        this.threshold = Integer.parseInt(properties.getProperty(PAR_THRESHOLD, DEFAULT_THRESHOLD));
        this.removeTimeWindow = Long.parseLong(properties.getProperty(PAR_DELIVERY_TIMEOUT, DEFAULT_DELIVERY_TIMEOUT));
        this.resolution = parseResolution(properties.getProperty(
                PAR_PEER_ADDRESS_RESOLUTION, DEFAULT_PEER_ADDRESS_RESOLUTION));
        this.portOffset = Integer.parseInt(properties.getProperty(PAR_PORT_OFFSET, DEFAULT_PORT_OFFSET));

        this.sentMessagesCounter = registerMetric(
                new Counter.Builder("SentMessages", Metric.Unit.NONE).build());

        if (resolution != Resolution.SHARED) {
            this.managingChannel = true;
            String address = properties.getProperty(PAR_CHANNEL_ADDRESS);
            String port = properties.getProperty(PAR_CHANNEL_PORT);
            if (address == null) {
                Objects.requireNonNull(myself, "myself must be set when " + PAR_CHANNEL_ADDRESS + " is unset");
                address = myself.getAddress().getHostAddress();
            }
            if (port == null) {
                Objects.requireNonNull(myself, "myself must be set when " + PAR_CHANNEL_PORT + " is unset");
                this.networkPort = myself.getPort();
                port = Integer.toString(this.networkPort);
            } else {
                this.networkPort = Integer.parseInt(port);
            }
            // fixed mode: peers are assumed to listen on PeerPort (defaults to our own channel port).
            this.peerPort = Integer.parseInt(
                    properties.getProperty(PAR_PEER_PORT, Integer.toString(this.networkPort)));
            this.myself = myself != null ? myself : new Host(InetAddress.getByName(address), this.networkPort);

            Properties channelProps = new Properties();
            channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
            channelProps.setProperty(TCPChannel.PORT_KEY, port);
            this.channelId = createChannel(TCPChannel.NAME, channelProps);
            setDefaultChannel(this.channelId);
            registerSerializersAndHandlers();
            logger.debug("Own channel id={} bound to {}:{} (resolution={})", channelId, address, port, resolution);
        } else {
            this.managingChannel = false;
            this.peerPort = -1; // unused in shared mode
            subscribeNotification(ChannelAvailableNotification.NOTIFICATION_ID, this::uponChannelAvailable);
            logger.debug("Shared-channel mode: waiting for a ChannelAvailableNotification");
        }

        subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUp);
        subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDown);

        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);
        registerTimerHandler(IHaveTimeout.TIMER_CODE, this::uponIHaveTimeout);
        registerTimerHandler(DispatchTimer.TIMER_CODE, this::uponDispatchTimer);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        if (managingChannel) {
            triggerNotification(new ChannelAvailableNotification(
                    PROTOCOL_ID, PROTOCOL_NAME, channelId, TCPChannel.NAME, myself));
        }
        if (dispatchPeriod > 0) {
            setupPeriodicTimer(new DispatchTimer(), dispatchPeriod, dispatchPeriod);
        }
    }

    /* ───────────────────────── Request handlers ──────────────────────── */

    // upon event Broadcast(m)  [Algorithm 1, lines 18-23]
    private void uponBroadcastRequest(BroadcastRequest request, short protoID) {
        GossipMessage msg = new GossipMessage(request.getTimestamp(), myself, request.getPayload());
        logger.debug("Broadcast {} (eager={}, lazy={})", msg.getMID(), eagerPushPeers.size(), lazyPushPeers.size());

        deliver(msg.clone());
        eagerPush(msg, 0, myself);
        lazyPush(msg.getMID(), 0, myself);
        cleanUp();
    }

    /* ───────────────────────── Message handlers ──────────────────────── */

    // upon event Receive(GOSSIP, ...)  [Algorithm 1, lines 24-38] (+ optional Optimize, Alg. 4)
    private void uponGossipMessage(GossipMessage msg, Host from, short sourceProto, int cID) {
        UUID mid = msg.getMID();
        int round = msg.getRound();

        if (!receivedTimestamps.containsKey(mid)) {
            logger.debug("New {} from {} (round {})", mid, from, round);
            deliver(msg.clone());

            Long tid = timers.remove(mid);
            if (tid != null) {
                cancelTimer(tid);
            }

            eagerPush(msg, round + 1, from);
            lazyPush(mid, round + 1, from);
            addToEager(from);

            if (optimization) {
                optimize(mid, round, from);
            }
            missing.remove(mid); // we now have it; drop any pending announcements
            cleanUp();
        } else {
            logger.trace("Duplicate {} from {} — pruning", mid, from);
            addToLazy(from);
            sendMessage(new PruneMessage(), from);
            sentMessagesCounter.inc();
        }
    }

    // upon event Receive(PRUNE, sender)  [Algorithm 1, lines 39-41]
    private void uponPruneMessage(PruneMessage msg, Host from, short sourceProto, int cID) {
        logger.trace("PRUNE from {}", from);
        addToLazy(from);
    }

    // upon event Receive(IHAVE, ...)  [Algorithm 2, lines 1-5]
    private void uponIHaveMessage(IHaveMessage msg, Host from, short sourceProto, int cID) {
        for (Announcement a : msg.getAnnouncements()) {
            UUID mid = a.messageId();
            if (receivedTimestamps.containsKey(mid)) {
                continue; // already have it
            }
            missing.computeIfAbsent(mid, k -> new ArrayDeque<>()).addLast(new Announce(from, a.round()));
            if (!timers.containsKey(mid)) {
                long tid = setupTimer(new IHaveTimeout(mid), timeout1);
                timers.put(mid, tid);
                logger.trace("Armed recovery timer for missing {} (announced by {})", mid, from);
            }
        }
    }

    // upon event Receive(GRAFT, ...)  [Algorithm 2, lines 12-16]
    private void uponGraftMessage(GraftMessage msg, Host from, short sourceProto, int cID) {
        logger.trace("GRAFT from {} for {}", from, msg.getMessageId());
        addToEager(from);

        UUID mid = msg.getMessageId();
        if (mid != null) {
            GossipMessage have = stored.get(mid);
            if (have != null) {
                GossipMessage out = have.clone();
                out.setRound(msg.getRound());
                sendMessage(out, from);
                sentMessagesCounter.inc();
                logger.debug("Served grafted {} to {}", mid, from);
            } else {
                logger.debug("Cannot serve grafted {} to {} — outside delivery window", mid, from);
            }
        }
    }

    /* ───────────────────────────── Timers ─────────────────────────────── */

    // upon Timer(mID)  [Algorithm 2, lines 6-11]
    private void uponIHaveTimeout(IHaveTimeout timer, long timerId) {
        UUID mid = timer.getMessageId();
        if (receivedTimestamps.containsKey(mid)) {
            timers.remove(mid);
            return; // arrived meanwhile
        }
        Deque<Announce> anns = missing.get(mid);
        if (anns == null || anns.isEmpty()) {
            timers.remove(mid);
            missing.remove(mid);
            return; // no one left to ask
        }

        // Re-arm with the shorter timeout, then ask the next announcer.
        long tid = setupTimer(new IHaveTimeout(mid), timeout2);
        timers.put(mid, tid);

        Announce a = anns.pollFirst();
        addToEager(a.sender());
        sendMessage(new GraftMessage(mid, a.round()), a.sender());
        sentMessagesCounter.inc();
        logger.debug("Recovery: grafted {} for missing {}", a.sender(), mid);
    }

    private void uponDispatchTimer(DispatchTimer timer, long timerId) {
        dispatch();
    }

    /* ───────────────────── Notification handlers ─────────────────────── */

    private void uponChannelAvailable(ChannelAvailableNotification event, short protoID) {
        if (channelReady) {
            return; // already attached
        }
        this.channelId = event.getChannelID();
        this.myself = event.getChannelListenData();
        this.networkPort = myself.getPort();
        registerSharedChannel(channelId);
        try {
            registerSerializersAndHandlers();
        } catch (HandlerRegistrationException ex) {
            // Without a usable channel the protocol cannot disseminate anything;
            // failing loudly is more honest than silently going dark.
            logger.fatal("Failed to register handlers on shared channel {}", channelId, ex);
            System.exit(1);
        }
        logger.debug("Attached to shared channel id={} owned by {} ({}), endpoint {}",
                channelId, event.getProtoSourceName(), event.getProtoSource(), myself);
    }

    // upon event NeighborUp(node)  [Algorithm 3, lines 6-7]
    private void uponNeighborUp(NeighborUp up, short protoID) {
        Host h = neighborHost(up.getPeer());
        neighbors.add(h);
        logger.debug("NeighborUp {}", h);

        if (managingChannel) {
            if (!eagerPushPeers.contains(h) && !lazyPushPeers.contains(h) && pending.add(h)) {
                openConnection(h);
            }
        } else {
            // Shared channel: the owner already holds the connection; the new
            // neighbour is a candidate tree branch right away.
            addToEager(h);
        }
    }

    // upon event NeighborDown(node)  [Algorithm 3, lines 1-5]
    private void uponNeighborDown(NeighborDown down, short protoID) {
        Host h = neighborHost(down.getPeer());
        neighbors.remove(h);
        logger.debug("NeighborDown {}", h);

        boolean wasConnected = eagerPushPeers.contains(h) || lazyPushPeers.contains(h);
        removePeer(h);
        if (managingChannel && (wasConnected || pending.remove(h))) {
            closeConnection(h);
        }
    }

    /* ───────────────────────── Channel events ─────────────────────────── */

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host h = event.getNode();
        logger.trace("Out-connection to {} up", h);
        if (!managingChannel) {
            return;
        }
        pending.remove(h);
        if (neighbors.contains(h)) {
            addToEager(h); // a freshly connected neighbour is a candidate tree branch
        } else {
            // Membership churn raced us; drop the connection.
            removePeer(h);
            closeConnection(h);
        }
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host h = event.getNode();
        logger.info("Out-connection to {} down: {}", h, event.getCause());
        reconnectOrDrop(h);
    }

    private void uponOutConnectionFailed(OutConnectionFailed<?> event, int channelId) {
        Host h = event.getNode();
        logger.info("Out-connection to {} failed: {}", h, event.getCause());
        reconnectOrDrop(h);
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("In-connection from {} up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("In-connection from {} down: {}", event.getNode(), event.getCause());
    }

    /* ────────────────────────── Public access ────────────────────────── */

    /** @return this node's {@link Host}; may be {@code null} in shared mode before attach. */
    public Host getHost() {
        return myself;
    }

    /* ───────────────────── Plumtree core procedures ──────────────────── */

    // procedure EagerPush(m, mID, round, sender)  [Algorithm 1, lines 5-7]
    private void eagerPush(GossipMessage msg, int round, Host sender) {
        for (Host p : eagerPushPeers) {
            if (p.equals(sender)) {
                continue;
            }
            GossipMessage out = msg.clone();
            out.setRound(round);
            sendMessage(out, p);
            sentMessagesCounter.inc();
        }
    }

    // procedure LazyPush(m, mID, round, sender)  [Algorithm 1, lines 8-11]
    private void lazyPush(UUID mid, int round, Host sender) {
        for (Host p : lazyPushPeers) {
            if (p.equals(sender)) {
                continue;
            }
            lazyQueue.computeIfAbsent(p, k -> new ArrayList<>()).add(new Announcement(mid, round));
        }
        if (dispatchPeriod <= 0) {
            dispatch(); // immediate policy
        }
    }

    // procedure dispatch  [Algorithm 1, lines 1-4]
    private void dispatch() {
        if (!channelReady || lazyQueue.isEmpty()) {
            return;
        }
        for (Map.Entry<Host, List<Announcement>> e : lazyQueue.entrySet()) {
            if (e.getValue().isEmpty()) {
                continue;
            }
            sendMessage(new IHaveMessage(new ArrayList<>(e.getValue())), e.getKey());
            sentMessagesCounter.inc();
        }
        lazyQueue.clear();
    }

    // procedure Optimization(mID, round, sender)  [Algorithm 4]
    private void optimize(UUID mid, int round, Host eagerSender) {
        Deque<Announce> anns = missing.get(mid);
        if (anns == null) {
            return;
        }
        for (Announce a : anns) {
            int r = a.round();
            if (r < round && (round - r) >= threshold) {
                Host better = a.sender();
                logger.debug("Optimize {}: swap eager {} (round {}) for lazy {} (round {})",
                        mid, eagerSender, round, better, r);
                // Promote the shorter lazy link, demote the longer eager one.
                addToEager(better);
                sendMessage(new GraftMessage(null, r), better); // null mid: promote only
                sentMessagesCounter.inc();
                addToLazy(eagerSender);
                sendMessage(new PruneMessage(), eagerSender);
                sentMessagesCounter.inc();
                return;
            }
        }
    }

    /* ────────────────────────────── Helpers ──────────────────────────── */

    private void deliver(GossipMessage msg) {
        UUID mid = msg.getMID();
        receivedInOrder.addLast(mid);
        receivedTimestamps.put(mid, System.currentTimeMillis());
        stored.put(mid, msg);
        triggerNotification(new BroadcastDelivery(msg.getSender(), msg.getPayload(), msg.getTimestamp()));
    }

    private void addToEager(Host h) {
        lazyPushPeers.remove(h);
        eagerPushPeers.add(h);
    }

    private void addToLazy(Host h) {
        eagerPushPeers.remove(h);
        lazyPushPeers.add(h);
    }

    /** Remove a peer from both sets and from any pending recovery state. */
    private void removePeer(Host h) {
        eagerPushPeers.remove(h);
        lazyPushPeers.remove(h);
        lazyQueue.remove(h);
        for (Iterator<Map.Entry<UUID, Deque<Announce>>> it = missing.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<UUID, Deque<Announce>> e = it.next();
            e.getValue().removeIf(a -> a.sender().equals(h));
            if (e.getValue().isEmpty()) {
                Long tid = timers.remove(e.getKey());
                if (tid != null) {
                    cancelTimer(tid);
                }
                it.remove();
            }
        }
    }

    private void reconnectOrDrop(Host h) {
        if (!managingChannel) {
            return; // the channel owner deals with it; we wait for NeighborDown
        }
        removePeer(h);
        if (neighbors.contains(h)) {
            pending.add(h);
            openConnection(h);
        } else {
            pending.remove(h);
            closeConnection(h);
        }
    }

    /**
     * Garbage-collect delivered messages older than the delivery window. The
     * buffer is append-only in (monotonic) delivery-time order, so a single
     * poll from the head per stale entry is sufficient.
     */
    private void cleanUp() {
        long staleBarrier = System.currentTimeMillis() - removeTimeWindow;
        while (!receivedInOrder.isEmpty()) {
            UUID head = receivedInOrder.peekFirst();
            Long ts = receivedTimestamps.get(head);
            if (ts == null || ts < staleBarrier) {
                receivedInOrder.pollFirst();
                receivedTimestamps.remove(head);
                stored.remove(head);
            } else {
                break;
            }
        }
    }

    /**
     * Map a membership-reported peer endpoint to the endpoint Plumtree talks to
     * it on, per {@link #PAR_PEER_ADDRESS_RESOLUTION}: {@code offset} adds
     * {@link #portOffset} to the peer's membership port, {@code fixed} uses the
     * uniform {@link #peerPort}, and {@code shared} reuses the membership
     * endpoint unchanged (same channel).
     */
    private Host neighborHost(Host peer) {
        return switch (resolution) {
            case OFFSET -> new Host(peer.getAddress(), peer.getPort() + portOffset);
            case FIXED -> new Host(peer.getAddress(), peerPort);
            case SHARED -> peer;
        };
    }

    private static Resolution parseResolution(String raw) {
        String v = raw == null ? "" : raw.trim().toLowerCase();
        return switch (v) {
            case RESOLUTION_OFFSET -> Resolution.OFFSET;
            case RESOLUTION_FIXED -> Resolution.FIXED;
            case RESOLUTION_SHARED -> Resolution.SHARED;
            default -> throw new IllegalArgumentException(PAR_PEER_ADDRESS_RESOLUTION
                    + " must be one of '" + RESOLUTION_OFFSET + "', '" + RESOLUTION_FIXED
                    + "', '" + RESOLUTION_SHARED + "' (was: '" + raw + "')");
        };
    }

    private void registerSerializersAndHandlers() throws HandlerRegistrationException {
        registerMessageSerializer(channelId, GossipMessage.MSG_CODE, GossipMessage.serializer);
        registerMessageSerializer(channelId, PruneMessage.MSG_CODE, PruneMessage.serializer);
        registerMessageSerializer(channelId, GraftMessage.MSG_CODE, GraftMessage.serializer);
        registerMessageSerializer(channelId, IHaveMessage.MSG_CODE, IHaveMessage.serializer);

        registerMessageHandler(channelId, GossipMessage.MSG_CODE, this::uponGossipMessage);
        registerMessageHandler(channelId, PruneMessage.MSG_CODE, this::uponPruneMessage);
        registerMessageHandler(channelId, GraftMessage.MSG_CODE, this::uponGraftMessage);
        registerMessageHandler(channelId, IHaveMessage.MSG_CODE, this::uponIHaveMessage);

        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        this.channelReady = true;
    }

    private static boolean readBool(Properties p, String key, boolean defaultValue) {
        String v = p.getProperty(key);
        return v == null ? defaultValue : Boolean.parseBoolean(v);
    }
}
