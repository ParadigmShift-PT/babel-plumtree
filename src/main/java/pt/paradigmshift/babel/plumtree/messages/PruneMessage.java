package pt.paradigmshift.babel.plumtree.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * The {@code PRUNE} control message: sent back to a peer whose eager push
 * delivered a <em>duplicate</em>, asking it to move this node from its eager
 * set to its lazy set — pruning the redundant tree branch.
 *
 * <p>Shared by both protocols. In the single-tree {@code Plumtree} the prune is
 * global (one shared tree) and {@link #getRoot() root} is {@code null}; in
 * {@code MultiPlumtree} it is scoped to a single per-source tree and
 * {@code root} carries that tree's originator.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}.
 */
public class PruneMessage extends ProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 3002;

    private final Host root;

    /** Construct a global (single-tree) prune. */
    public PruneMessage() {
        super(MSG_CODE);
        this.root = null;
    }

    /**
     * Construct a per-root prune.
     *
     * @param root the originator identifying the tree to prune, or {@code null}
     *             for the single shared tree
     */
    public PruneMessage(Host root) {
        super(MSG_CODE);
        this.root = root;
    }

    /** @return the tree root this prune is scoped to, or {@code null} */
    public Host getRoot() {
        return root;
    }

    @Override
    public String toString() {
        return "PruneMessage{root=" + root + '}';
    }

    public static final ISerializer<PruneMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PruneMessage msg, ByteBuf out) throws IOException {
            if (msg.root == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                Host.serializer.serialize(msg.root, out);
            }
        }

        @Override
        public PruneMessage deserialize(ByteBuf in) throws IOException {
            Host root = in.readBoolean() ? Host.serializer.deserialize(in) : null;
            return new PruneMessage(root);
        }
    };
}
