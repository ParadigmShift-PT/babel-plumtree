package pt.paradigmshift.babel.plumtree.messages;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.messages.IdentifiableProtoMessage;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.notifications.BroadcastDelivery;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * The eager-push payload message (the paper's {@code GOSSIP}). Carries one
 * application-level broadcast in full along the branches of the spanning tree.
 *
 * <p>Shared by both {@code Plumtree} and {@code MultiPlumtree}. The
 * {@link #getSender() sender} is the <b>original</b> broadcaster (it is set
 * once when the broadcast is issued and preserved across every hop) — it is
 * used to build the {@link BroadcastDelivery} notification, and in
 * {@code MultiPlumtree} it doubles as the <b>root</b> that identifies which
 * per-source tree this message belongs to. The {@link #getRound() round} is
 * the hop count from the originator (0 at the source); the protocol increments
 * it on every relay so receivers can reason about path length.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}. Numbered in the
 * {@code Plumtree} (slot 3000) message pool; {@code MultiPlumtree} (slot 3100)
 * deliberately reuses the same wire messages — see this project's README.
 */
public class GossipMessage extends IdentifiableProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 3001;

    private final Timestamp timestamp;
    private final Host sender;
    private final byte[] payload;
    private int round;
    private short protoID;

    /**
     * Construct a fresh broadcast initiated by this node: assigns a new
     * {@code UUID} message identifier and {@code round = 0}. Clones the sender
     * host and payload so later mutation by the caller cannot affect the
     * in-flight message.
     *
     * @param t   the timestamp the broadcast was issued at
     * @param s   the original sender (also the tree root in MultiPlumtree)
     * @param p   the opaque application payload
     * @param pID the identifier of the protocol that issued the broadcast
     */
    public GossipMessage(Timestamp t, Host s, byte[] p, short pID) {
        super(GossipMessage.MSG_CODE);
        this.timestamp = Timestamp.from(t.toInstant());
        this.sender = new Host(s.getAddress(), s.getPort());
        this.payload = p.clone();
        this.round = 0;
        this.protoID = pID;
    }

    /** Deserializer target. */
    private GossipMessage(UUID mid, long t, Host s, byte[] p, int round, short pID) {
        super(GossipMessage.MSG_CODE, mid);
        this.timestamp = new Timestamp(t);
        this.sender = s;
        this.payload = p;
        this.round = round;
        this.protoID = pID;
    }

    /** Copy-constructor used by {@link #clone()}. Shares the payload array. */
    private GossipMessage(GossipMessage m) {
        super(GossipMessage.MSG_CODE, m.getMID());
        this.timestamp = m.timestamp;
        this.sender = m.sender;
        this.payload = m.payload;
        this.round = m.round;
        this.protoID = m.protoID;
    }

    @Override
    public BroadcastDelivery generateDeliveryNotification(short sourceProtoID) {
        return new BroadcastDelivery(sender, payload.clone(), new Timestamp(timestamp.getTime()));
    }

    /**
     * Shallow clone sharing the payload byte array. The protocols send clones
     * so each send-loop iteration carries an independent {@code round} value.
     */
    @Override
    public GossipMessage clone() {
        return new GossipMessage(this);
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    /** @return the original broadcaster (and, in MultiPlumtree, the tree root) */
    public Host getSender() {
        return sender;
    }

    public byte[] getPayload() {
        return payload;
    }

    /** @return the hop count from the originator (0 at the source) */
    public int getRound() {
        return round;
    }

    public void setRound(int round) {
        this.round = round;
    }

    public short getProtoID() {
        return protoID;
    }

    public void setProtoID(short protoID) {
        this.protoID = protoID;
    }

    @Override
    public String toString() {
        return "GossipMessage{"
                + "sender=" + sender
                + ", mid=" + getMID()
                + ", round=" + round
                + ", timestamp=" + timestamp
                + ", payload size=" + payload.length
                + ", protoID=" + protoID
                + '}';
    }

    public static final ISerializer<GossipMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(GossipMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.getMID().getMostSignificantBits());
            out.writeLong(msg.getMID().getLeastSignificantBits());
            Host.serializer.serialize(msg.sender, out);
            out.writeLong(msg.timestamp.getTime());
            out.writeInt(msg.round);
            out.writeShort(msg.protoID);
            out.writeInt(msg.payload.length);
            out.writeBytes(msg.payload);
        }

        @Override
        public GossipMessage deserialize(ByteBuf in) throws IOException {
            UUID mid = new UUID(in.readLong(), in.readLong());
            Host origin = Host.serializer.deserialize(in);
            long t = in.readLong();
            int round = in.readInt();
            short pid = in.readShort();
            int len = in.readInt();
            if (len < 0 || len > in.readableBytes()) {
                throw new IOException("GossipMessage: payload length out of range: " + len);
            }
            byte[] payload = new byte[len];
            in.readBytes(payload);
            return new GossipMessage(mid, t, origin, payload, round, pid);
        }
    };
}
