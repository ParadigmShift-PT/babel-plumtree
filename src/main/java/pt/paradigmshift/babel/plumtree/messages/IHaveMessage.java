package pt.paradigmshift.babel.plumtree.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * The {@code IHAVE} lazy-push message: announces, by identifier only (never the
 * payload), one or more messages the sender holds. A receiver that is missing
 * an announced message starts a recovery timer (single-tree {@code Plumtree}) or
 * grafts immediately ({@code MultiPlumtree}).
 *
 * <p>Announcements are <b>batched</b> — the paper's scheduling policy
 * piggybacks several {@code IHAVE}s into one control message to reduce traffic;
 * the only requirement is that every queued announcement is eventually sent.
 *
 * <p>{@link #getRoot() root} is {@code null} for the single-tree protocol;
 * {@code MultiPlumtree} sends one {@code IHAVE} per (peer, root) and
 * fills in the originator.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}.
 */
public class IHaveMessage extends ProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 3004;

    private final Host root;
    private final List<Announcement> announcements;

    /** Construct a single-tree batch (no root). */
    public IHaveMessage(List<Announcement> announcements) {
        this(null, announcements);
    }

    /**
     * Construct a per-root batch.
     *
     * @param root          the tree originator, or {@code null} for the shared tree
     * @param announcements the (messageId, round) pairs being announced
     */
    public IHaveMessage(Host root, List<Announcement> announcements) {
        super(MSG_CODE);
        this.root = root;
        this.announcements = announcements;
    }

    /** @return the tree root these announcements belong to, or {@code null} */
    public Host getRoot() {
        return root;
    }

    public List<Announcement> getAnnouncements() {
        return announcements;
    }

    @Override
    public String toString() {
        return "IHaveMessage{root=" + root + ", announcements=" + announcements.size() + '}';
    }

    public static final ISerializer<IHaveMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(IHaveMessage msg, ByteBuf out) throws IOException {
            if (msg.root == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                Host.serializer.serialize(msg.root, out);
            }
            out.writeInt(msg.announcements.size());
            for (Announcement a : msg.announcements) {
                out.writeLong(a.messageId().getMostSignificantBits());
                out.writeLong(a.messageId().getLeastSignificantBits());
                out.writeInt(a.round());
            }
        }

        @Override
        public IHaveMessage deserialize(ByteBuf in) throws IOException {
            Host root = in.readBoolean() ? Host.serializer.deserialize(in) : null;
            int count = in.readInt();
            if (count < 0) {
                throw new IOException("IHaveMessage: negative announcement count " + count);
            }
            List<Announcement> announcements = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                UUID mid = new UUID(in.readLong(), in.readLong());
                int round = in.readInt();
                announcements.add(new Announcement(mid, round));
            }
            return new IHaveMessage(root, announcements);
        }
    };
}
