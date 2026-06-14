package pt.paradigmshift.babel.plumtree.timers;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

/**
 * Per-message recovery timer used by the single-tree {@code Plumtree} (paper
 * Algorithm 2). Armed when an {@code IHAVE} announces a message this node has
 * not yet received; if it fires before the payload arrives by eager push, the
 * node grafts the first announcer to request the message. Carries the
 * identifier of the message it is waiting for.
 *
 * <p>Handler class: timer. <b>ID:</b> {@value #TIMER_CODE}. Owning protocol:
 * {@code Plumtree} (id 3000).
 */
public class IHaveTimeout extends ProtoTimer {

    /** Babel timer numeric identifier. */
    public static final short TIMER_CODE = 3001;

    private final UUID messageId;

    /** Arm a recovery timer for the message whose {@code IHAVE} was just received. */
    public IHaveTimeout(UUID messageId) {
        super(TIMER_CODE);
        this.messageId = messageId;
    }

    /** @return the id of the message this timer is waiting to receive */
    public UUID getMessageId() {
        return messageId;
    }

    /** Stateful timer: the message id is immutable, so a fresh wrapper suffices. */
    @Override
    public ProtoTimer clone() {
        return new IHaveTimeout(messageId);
    }
}
