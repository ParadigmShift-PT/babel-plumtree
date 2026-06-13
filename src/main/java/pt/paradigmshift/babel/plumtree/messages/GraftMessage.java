package pt.paradigmshift.babel.plumtree.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * The {@code GRAFT} control message, with a dual purpose (paper Algorithm 2):
 * it (a) re-attaches the link to the spanning tree — the receiver promotes the
 * sender into its eager set — and (b) requests retransmission of a specific
 * message. The receiver replies with the corresponding {@link GossipMessage}
 * if it still holds it.
 *
 * <p>The {@link #getMessageId() messageId} may be {@code null}: the single-tree
 * {@code Plumtree} tree-optimization (Algorithm 4) sends a payload-less graft
 * purely to promote a shorter lazy link to eager, without requesting any
 * retransmission.
 *
 * <p>{@link #getRoot() root} is {@code null} for the single-tree protocol and
 * carries the tree originator for {@code MultiPlumtree}.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}.
 */
public class GraftMessage extends ProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 3003;

    private final UUID messageId;
    private final int round;
    private final Host root;

    /**
     * Construct a single-tree graft.
     *
     * @param messageId the message to retransmit, or {@code null} to only
     *                  promote the link (optimization graft)
     * @param round     the round to stamp on the retransmitted gossip
     */
    public GraftMessage(UUID messageId, int round) {
        this(messageId, round, null);
    }

    /**
     * Construct a per-root graft.
     *
     * @param messageId the message to retransmit, or {@code null}
     * @param round     the round to stamp on the retransmitted gossip
     * @param root      the originator identifying the tree, or {@code null}
     */
    public GraftMessage(UUID messageId, int round, Host root) {
        super(MSG_CODE);
        this.messageId = messageId;
        this.round = round;
        this.root = root;
    }

    /** @return the requested message id, or {@code null} for a promote-only graft */
    public UUID getMessageId() {
        return messageId;
    }

    public int getRound() {
        return round;
    }

    /** @return the tree root this graft is scoped to, or {@code null} */
    public Host getRoot() {
        return root;
    }

    @Override
    public String toString() {
        return "GraftMessage{mid=" + messageId + ", round=" + round + ", root=" + root + '}';
    }

    public static final ISerializer<GraftMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(GraftMessage msg, ByteBuf out) throws IOException {
            if (msg.messageId == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeLong(msg.messageId.getMostSignificantBits());
                out.writeLong(msg.messageId.getLeastSignificantBits());
            }
            out.writeInt(msg.round);
            if (msg.root == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                Host.serializer.serialize(msg.root, out);
            }
        }

        @Override
        public GraftMessage deserialize(ByteBuf in) throws IOException {
            UUID mid = in.readBoolean() ? new UUID(in.readLong(), in.readLong()) : null;
            int round = in.readInt();
            Host root = in.readBoolean() ? Host.serializer.deserialize(in) : null;
            return new GraftMessage(mid, round, root);
        }
    };
}
