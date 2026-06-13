package pt.paradigmshift.babel.plumtree;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
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
import pt.paradigmshift.babel.plumtree.messages.IgnoredIHaveMessage;
import pt.paradigmshift.babel.plumtree.messages.PruneMessage;
import pt.paradigmshift.babel.plumtree.timers.LazyTickTimer;
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
 * MultiPlumtree — the multiple-trees variant of the push-lazy-push multicast
 * tree broadcast protocol.
 *
 * <p>An independent ParadigmShift implementation following:
 * <blockquote>
 * João Leitão, José Pereira, and Luís Rodrigues. <i>Epidemic Broadcast
 * Trees</i>. In Proc. 26th IEEE Symposium on Reliable Distributed Systems
 * (SRDS 2007), pp. 301–310, 2007. doi:10.1109/SRDS.2007.27.
 * </blockquote>
 * Where {@link Plumtree} builds a <b>single</b> spanning tree shared by all
 * senders (optimized for whoever broadcasts first), this variant realizes the
 * paper's §3.7 observation that, for optimal latency, <i>a distinct tree may be
 * maintained per source</i>. Instead of running a separate protocol instance
 * per sender, it keeps <b>per-root</b> eager/lazy peer sets within one
 * instance: the originator of each broadcast (the message's {@code sender},
 * carried as the <i>root</i>) identifies which tree the eager/lazy partition
 * applies to, so every source converges to its own latency-optimal tree.
 *
 * <h2>Tree construction and repair</h2>
 * For a given root, novel {@link GossipMessage}s are eager-pushed along that
 * root's tree and the sender is grafted into the root's eager set; duplicates
 * prune the sender into the root's lazy set (with a {@link PruneMessage}
 * carrying the root). Lazy push is <b>queued and periodic</b>: each lazy
 * announcement is parked in a per-peer pending queue and flushed on a {@link
 * LazyTickTimer} tick as batched {@link IHaveMessage}s (one per root). An
 * announcement is re-sent on every tick until the peer either {@link
 * GraftMessage grafts} the message (it was missing — repairing that root's
 * tree) or acknowledges it with an {@link IgnoredIHaveMessage} (it already had
 * it). This standing re-advertisement is what makes lazy push reliable across
 * control-message loss without a per-message recovery timer.
 *
 * <h2>Channel modes</h2>
 * Identical to {@link Plumtree}: by default the protocol owns and announces its
 * own {@link TCPChannel}; with {@code MultiPlumtree.UseSharedChannel=true} it
 * attaches to a channel announced by another protocol (e.g. HyParView) and
 * leaves connection management to that owner.
 *
 * <h2>Acknowledgement</h2>
 * The per-root sets, the periodic lazy tick over a standing queue of
 * outstanding announcements, and the {@link IgnoredIHaveMessage}
 * acknowledgement that retires them go beyond the paper, whose lazy push is a
 * one-shot per-message timer. This machinery follows the design of Riak's
 * {@code riak_core_broadcast} module, originally implemented by Jordan West —
 * <a href="https://github.com/basho/riak_core/blob/develop/src/riak_core_broadcast.erl">riak_core_broadcast.erl</a>.
 * No code from that project is reused here; with thanks to Jordan West and the
 * riak_core authors.
 *
 * @see Plumtree
 * @see GossipMessage
 */
public class MultiPlumtree extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(MultiPlumtree.class);

    /** Babel protocol numeric identifier. */
    public static final short PROTOCOL_ID = 3100;
    /** Babel protocol name. */
    public static final String PROTOCOL_NAME = "MultiPlumtree";

    /** Property — attach to a channel announced by another protocol instead of opening one. */
    public static final String PAR_USE_SHARED_CHANNEL = "MultiPlumtree.UseSharedChannel";
    /** Default for {@link #PAR_USE_SHARED_CHANNEL}: {@value}. */
    public static final boolean DEFAULT_USE_SHARED_CHANNEL = false;

    /** Property — TCP bind address (own-channel mode). Defaults to the {@code myself} host. */
    public static final String PAR_CHANNEL_ADDRESS = "MultiPlumtree.Channel.Address";
    /** Property — TCP bind port (own-channel mode). Defaults to the {@code myself} host. */
    public static final String PAR_CHANNEL_PORT = "MultiPlumtree.Channel.Port";

    /**
     * Property — lazy-push tick period in ms: how often the pending lazy queue
     * is flushed as batched {@code IHAVE} announcements (and, since entries
     * persist until acknowledged, how often unacknowledged announcements are
     * retried). The repair latency for a broken tree branch is bounded by this
     * period.
     */
    public static final String PAR_LAZY_TICK_PERIOD = "MultiPlumtree.LazyTickPeriod";
    /** Default lazy-push tick period: {@value} ms. */
    public static final String DEFAULT_LAZY_TICK_PERIOD = "1000";

    /** Property — how long a delivered message is remembered (dedup + graft-replay window). */
    public static final String PAR_DELIVERY_TIMEOUT = "MultiPlumtree.DeliveredTimeout";
    /** Default delivery-window timeout: {@value} ms (10 minutes). */
    public static final String DEFAULT_DELIVERY_TIMEOUT = "600000";

    /** Property — single-host test support: neighbour MultiPlumtree port is {@code peer.port + 1}. */
    public static final String PAR_LOCAL_SUPPORT = "MultiPlumtree.LocalSupport";
    /** Default for {@link #PAR_LOCAL_SUPPORT}: {@value}. */
    public static final boolean DEFAULT_LOCAL_SUPPORT = false;

    private final long lazyTickPeriod;
    private final long removeTimeWindow;
    private final boolean localSupport;

    private final boolean managingChannel;
    private boolean channelReady;
    private int channelId;
    private int networkPort;
    private Host myself;

    /** A parked lazy announcement: the round it was seen at and the tree it belongs to. */
    private record LazyEntry(int round, Host root) { }

    /**
     * Current membership (from NeighborUp/Down), in MultiPlumtree-endpoint
     * space. Doubles as the initial eager set for any tree root not yet seen.
     */
    private final Set<Host> neighbors = new HashSet<>();
    /** Own-channel mode: neighbours we are connecting to but have not confirmed yet. */
    private final Set<Host> pending = new HashSet<>();

    /** Per-root tree branches: root → peers we eager-push that root's messages to. */
    private final Map<Host, Set<Host>> eagerSets = new HashMap<>();
    /** Per-root non-tree links: root → peers we only announce that root's messages to. */
    private final Map<Host, Set<Host>> lazySets = new HashMap<>();

    /** FIFO of delivered message ids, oldest first — drives the delivery-window GC. */
    private final LinkedList<UUID> receivedInOrder = new LinkedList<>();
    /** Delivered message id → local delivery time (dedup + GC). */
    private final Map<UUID, Long> receivedTimestamps = new HashMap<>();
    /** Delivered message id → full message, retained within the window to answer grafts. */
    private final Map<UUID, GossipMessage> stored = new HashMap<>();

    /**
     * Pending lazy announcements: peer → (message id → its round and root).
     * Flushed every {@link LazyTickTimer}; an entry is cleared once the peer
     * grafts the message or acknowledges it via {@link IgnoredIHaveMessage}.
     */
    private final Map<Host, Map<UUID, LazyEntry>> outstanding = new HashMap<>();

    private final Counter sentMessagesCounter;

    /**
     * Construct the protocol and wire its handlers. Depending on
     * {@link #PAR_USE_SHARED_CHANNEL} it either binds its own TCP channel now
     * or subscribes to {@link ChannelAvailableNotification} to attach to a
     * shared one.
     *
     * @param properties protocol configuration; see the {@code PAR_*} constants
     * @param myself     this node's {@link Host} identity. May be {@code null}
     *                   in shared-channel mode; required otherwise unless
     *                   {@code Channel.Address} + {@code Channel.Port} are set.
     */
    public MultiPlumtree(Properties properties, Host myself)
            throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;

        this.lazyTickPeriod = Long.parseLong(properties.getProperty(PAR_LAZY_TICK_PERIOD, DEFAULT_LAZY_TICK_PERIOD));
        this.removeTimeWindow = Long.parseLong(properties.getProperty(PAR_DELIVERY_TIMEOUT, DEFAULT_DELIVERY_TIMEOUT));
        this.localSupport = readBool(properties, PAR_LOCAL_SUPPORT, DEFAULT_LOCAL_SUPPORT);

        this.sentMessagesCounter = registerMetric(
                new Counter.Builder("SentMessages", Metric.Unit.NONE).build());

        boolean wantShared = readBool(properties, PAR_USE_SHARED_CHANNEL, DEFAULT_USE_SHARED_CHANNEL);
        if (!wantShared) {
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
            this.myself = myself != null ? myself : new Host(InetAddress.getByName(address), this.networkPort);

            Properties channelProps = new Properties();
            channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
            channelProps.setProperty(TCPChannel.PORT_KEY, port);
            this.channelId = createChannel(TCPChannel.NAME, channelProps);
            setDefaultChannel(this.channelId);
            registerSerializersAndHandlers();
            logger.debug("Created own channel id={} bound to {}:{}", channelId, address, port);
        } else {
            this.managingChannel = false;
            subscribeNotification(ChannelAvailableNotification.NOTIFICATION_ID, this::uponChannelAvailable);
            logger.debug("Shared-channel mode: waiting for a ChannelAvailableNotification");
        }

        subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUp);
        subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDown);

        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);
        registerTimerHandler(LazyTickTimer.TIMER_CODE, this::uponLazyTick);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        if (managingChannel) {
            triggerNotification(new ChannelAvailableNotification(
                    PROTOCOL_ID, PROTOCOL_NAME, channelId, TCPChannel.NAME, myself));
        }
        setupPeriodicTimer(new LazyTickTimer(), lazyTickPeriod, lazyTickPeriod);
    }

    /* ───────────────────────── Request handlers ──────────────────────── */

    private void uponBroadcastRequest(BroadcastRequest request, short protoID) {
        GossipMessage msg = new GossipMessage(request.getTimestamp(), myself, request.getPayload(), protoID);
        Host root = myself; // we originate this tree
        logger.debug("Broadcast {} on own tree (eager={})", msg.getMID(), eagerPeers(root).size());

        deliver(msg.clone());
        eagerPush(msg, root, 0, myself);
        scheduleLazy(msg.getMID(), root, 0, myself);
        cleanUp();
    }

    /* ───────────────────────── Message handlers ──────────────────────── */

    private void uponGossipMessage(GossipMessage msg, Host from, short sourceProto, int cID) {
        UUID mid = msg.getMID();
        Host root = msg.getSender();
        int round = msg.getRound();

        if (!receivedTimestamps.containsKey(mid)) {
            logger.debug("New {} (root {}) from {} (round {})", mid, root, from, round);
            deliver(msg.clone());
            addEager(from, root);
            eagerPush(msg, root, round + 1, from);
            scheduleLazy(mid, root, round + 1, from);
            cleanUp();
        } else {
            logger.trace("Duplicate {} (root {}) from {} — pruning", mid, root, from);
            addLazy(from, root);
            sendMessage(new PruneMessage(root), from);
            sentMessagesCounter.inc();
        }
    }

    private void uponPruneMessage(PruneMessage msg, Host from, short sourceProto, int cID) {
        Host root = msg.getRoot();
        logger.trace("PRUNE from {} (root {})", from, root);
        addLazy(from, root);
    }

    private void uponIHaveMessage(IHaveMessage msg, Host from, short sourceProto, int cID) {
        Host root = msg.getRoot();
        List<Announcement> toAck = new ArrayList<>();

        for (Announcement a : msg.getAnnouncements()) {
            UUID mid = a.messageId();
            if (receivedTimestamps.containsKey(mid)) {
                // Already have it: acknowledge so the announcer stops re-sending.
                toAck.add(a);
            } else {
                // Missing: graft immediately and treat the announcer as a tree branch.
                addEager(from, root);
                sendMessage(new GraftMessage(mid, a.round(), root), from);
                sentMessagesCounter.inc();
                logger.debug("Grafted {} for missing {} (root {})", from, mid, root);
            }
        }
        if (!toAck.isEmpty()) {
            sendMessage(new IgnoredIHaveMessage(root, toAck), from);
            sentMessagesCounter.inc();
        }
    }

    private void uponGraftMessage(GraftMessage msg, Host from, short sourceProto, int cID) {
        Host root = msg.getRoot();
        UUID mid = msg.getMessageId();
        logger.trace("GRAFT from {} for {} (root {})", from, mid, root);

        addEager(from, root);
        if (mid != null) {
            // The grafter is getting (or already has) it — stop advertising it to them.
            removeOutstanding(from, mid);
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

    private void uponIgnoredIHaveMessage(IgnoredIHaveMessage msg, Host from, short sourceProto, int cID) {
        for (Announcement a : msg.getAnnouncements()) {
            removeOutstanding(from, a.messageId());
        }
    }

    /* ───────────────────────────── Timers ─────────────────────────────── */

    private void uponLazyTick(LazyTickTimer timer, long timerId) {
        if (!channelReady || outstanding.isEmpty()) {
            return;
        }
        for (Map.Entry<Host, Map<UUID, LazyEntry>> peerEntry : outstanding.entrySet()) {
            Host peer = peerEntry.getKey();
            Map<UUID, LazyEntry> entries = peerEntry.getValue();
            if (entries.isEmpty()) {
                continue;
            }
            // One batched IHAVE per root.
            Map<Host, List<Announcement>> byRoot = new HashMap<>();
            for (Map.Entry<UUID, LazyEntry> e : entries.entrySet()) {
                LazyEntry le = e.getValue();
                byRoot.computeIfAbsent(le.root(), k -> new ArrayList<>())
                        .add(new Announcement(e.getKey(), le.round()));
            }
            for (Map.Entry<Host, List<Announcement>> r : byRoot.entrySet()) {
                sendMessage(new IHaveMessage(r.getKey(), r.getValue()), peer);
                sentMessagesCounter.inc();
            }
        }
    }

    /* ───────────────────── Notification handlers ─────────────────────── */

    private void uponChannelAvailable(ChannelAvailableNotification event, short protoID) {
        if (channelReady) {
            return;
        }
        this.channelId = event.getChannelID();
        this.myself = event.getChannelListenData();
        this.networkPort = myself.getPort();
        registerSharedChannel(channelId);
        try {
            registerSerializersAndHandlers();
        } catch (HandlerRegistrationException ex) {
            logger.fatal("Failed to register handlers on shared channel {}", channelId, ex);
            System.exit(1);
        }
        logger.debug("Attached to shared channel id={} owned by {} ({}), endpoint {}",
                channelId, event.getProtoSourceName(), event.getProtoSource(), myself);
    }

    private void uponNeighborUp(NeighborUp up, short protoID) {
        Host h = neighborHost(up.getPeer());
        logger.debug("NeighborUp {}", h);

        if (managingChannel) {
            if (!neighbors.contains(h) && pending.add(h)) {
                openConnection(h);
            }
        } else {
            promoteNewMember(h);
        }
    }

    private void uponNeighborDown(NeighborDown down, short protoID) {
        Host h = neighborHost(down.getPeer());
        logger.debug("NeighborDown {}", h);

        boolean wasNeighbor = neighbors.remove(h);
        removeMemberFromTrees(h);
        if (managingChannel && (wasNeighbor || pending.remove(h))) {
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
        if (!pending.remove(h) && !neighbors.contains(h)) {
            closeConnection(h); // membership churn raced us
            return;
        }
        promoteNewMember(h);
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

    /* ──────────────────── Per-root tree procedures ───────────────────── */

    /** The eager set of {@code root}'s tree, defaulting to all current neighbours. */
    private Set<Host> eagerPeers(Host root) {
        return eagerSets.computeIfAbsent(root, k -> new HashSet<>(neighbors));
    }

    /** The lazy set of {@code root}'s tree, initially empty. */
    private Set<Host> lazyPeers(Host root) {
        return lazySets.computeIfAbsent(root, k -> new HashSet<>());
    }

    private void addEager(Host h, Host root) {
        lazyPeers(root).remove(h);
        eagerPeers(root).add(h);
    }

    private void addLazy(Host h, Host root) {
        eagerPeers(root).remove(h);
        lazyPeers(root).add(h);
    }

    private void eagerPush(GossipMessage msg, Host root, int round, Host sender) {
        for (Host p : eagerPeers(root)) {
            if (p.equals(sender)) {
                continue;
            }
            GossipMessage out = msg.clone();
            out.setRound(round);
            sendMessage(out, p);
            sentMessagesCounter.inc();
        }
    }

    private void scheduleLazy(UUID mid, Host root, int round, Host sender) {
        for (Host p : lazyPeers(root)) {
            if (p.equals(sender)) {
                continue;
            }
            outstanding.computeIfAbsent(p, k -> new HashMap<>()).put(mid, new LazyEntry(round, root));
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

    /** A newly available member becomes an eager candidate on every known tree. */
    private void promoteNewMember(Host h) {
        neighbors.add(h);
        for (Host root : eagerSets.keySet()) {
            eagerSets.get(root).add(h);
            Set<Host> lazy = lazySets.get(root);
            if (lazy != null) {
                lazy.remove(h);
            }
        }
    }

    /** Drop a departed member from every tree and stop tracking its lazy queue. */
    private void removeMemberFromTrees(Host h) {
        for (Set<Host> s : eagerSets.values()) {
            s.remove(h);
        }
        for (Set<Host> s : lazySets.values()) {
            s.remove(h);
        }
        outstanding.remove(h);
    }

    private void reconnectOrDrop(Host h) {
        if (!managingChannel) {
            return; // the channel owner deals with it; we wait for NeighborDown
        }
        removeMemberFromTrees(h);
        if (neighbors.contains(h)) {
            pending.add(h);
            openConnection(h);
        } else {
            pending.remove(h);
            closeConnection(h);
        }
    }

    private void removeOutstanding(Host peer, UUID mid) {
        Map<UUID, LazyEntry> m = outstanding.get(peer);
        if (m != null) {
            m.remove(mid);
            if (m.isEmpty()) {
                outstanding.remove(peer);
            }
        }
    }

    private void purgeOutstanding(UUID mid) {
        for (Iterator<Map.Entry<Host, Map<UUID, LazyEntry>>> it = outstanding.entrySet().iterator(); it.hasNext(); ) {
            Map<UUID, LazyEntry> m = it.next().getValue();
            m.remove(mid);
            if (m.isEmpty()) {
                it.remove();
            }
        }
    }

    /**
     * Garbage-collect delivered messages older than the delivery window, and
     * stop advertising anything that aged out.
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
                purgeOutstanding(head);
            } else {
                break;
            }
        }
    }

    /**
     * Map a membership-reported peer endpoint to the endpoint MultiPlumtree
     * talks to it on. In shared-channel mode that is the same endpoint; in
     * own-channel mode every node listens on {@link #networkPort} (or, under
     * {@link #localSupport}, {@code peer.port + 1} for single-host tests).
     */
    private Host neighborHost(Host peer) {
        if (!managingChannel) {
            return peer;
        }
        return new Host(peer.getAddress(), localSupport ? peer.getPort() + 1 : networkPort);
    }

    private void registerSerializersAndHandlers() throws HandlerRegistrationException {
        registerMessageSerializer(channelId, GossipMessage.MSG_CODE, GossipMessage.serializer);
        registerMessageSerializer(channelId, PruneMessage.MSG_CODE, PruneMessage.serializer);
        registerMessageSerializer(channelId, GraftMessage.MSG_CODE, GraftMessage.serializer);
        registerMessageSerializer(channelId, IHaveMessage.MSG_CODE, IHaveMessage.serializer);
        registerMessageSerializer(channelId, IgnoredIHaveMessage.MSG_CODE, IgnoredIHaveMessage.serializer);

        registerMessageHandler(channelId, GossipMessage.MSG_CODE, this::uponGossipMessage);
        registerMessageHandler(channelId, PruneMessage.MSG_CODE, this::uponPruneMessage);
        registerMessageHandler(channelId, GraftMessage.MSG_CODE, this::uponGraftMessage);
        registerMessageHandler(channelId, IHaveMessage.MSG_CODE, this::uponIHaveMessage);
        registerMessageHandler(channelId, IgnoredIHaveMessage.MSG_CODE, this::uponIgnoredIHaveMessage);

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
