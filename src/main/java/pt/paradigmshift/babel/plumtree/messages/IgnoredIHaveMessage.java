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
 * The {@code IGNORED_IHAVE} acknowledgement: sent in reply to an
 * {@link IHaveMessage} for messages the receiver <em>already holds</em>, so the
 * announcer can stop re-advertising them on its next lazy tick.
 *
 * <p>Used only by {@code MultiPlumtree}, whose lazy queue ({@code outstanding})
 * keeps re-announcing a message until each lazy peer either grafts it (does not
 * have it) or acknowledges it via this message (already has it). This bounds
 * the lazy traffic that the single-tree {@code Plumtree} instead bounds with a
 * one-shot dispatch.
 *
 * <p>This acknowledgement is not part of the original paper; it follows Riak's
 * {@code riak_core_broadcast} ({@code ignored_i_have}), originally implemented
 * by Jordan West. See {@link pt.paradigmshift.babel.plumtree.MultiPlumtree} for
 * the full acknowledgement and link.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}.
 */
public class IgnoredIHaveMessage extends ProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 3005;

    private final Host root;
    private final List<Announcement> announcements;

    /**
     * @param root          the tree originator the acknowledged announcements
     *                      belong to, or {@code null}
     * @param announcements the (messageId, round) pairs being acknowledged
     */
    public IgnoredIHaveMessage(Host root, List<Announcement> announcements) {
        super(MSG_CODE);
        this.root = root;
        this.announcements = announcements;
    }

    public Host getRoot() {
        return root;
    }

    public List<Announcement> getAnnouncements() {
        return announcements;
    }

    @Override
    public String toString() {
        return "IgnoredIHaveMessage{root=" + root + ", announcements=" + announcements.size() + '}';
    }

    public static final ISerializer<IgnoredIHaveMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(IgnoredIHaveMessage msg, ByteBuf out) throws IOException {
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
        public IgnoredIHaveMessage deserialize(ByteBuf in) throws IOException {
            Host root = in.readBoolean() ? Host.serializer.deserialize(in) : null;
            int count = in.readInt();
            if (count < 0) {
                throw new IOException("IgnoredIHaveMessage: negative announcement count " + count);
            }
            List<Announcement> announcements = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                UUID mid = new UUID(in.readLong(), in.readLong());
                int round = in.readInt();
                announcements.add(new Announcement(mid, round));
            }
            return new IgnoredIHaveMessage(root, announcements);
        }
    };
}
