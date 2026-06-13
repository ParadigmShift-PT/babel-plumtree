package pt.paradigmshift.babel.plumtree.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

/**
 * Periodic timer that drives {@code MultiPlumtree}'s lazy push. On each tick
 * the pending lazy-announcement queue is flushed:
 * for every peer with pending announcements, one batched {@link
 * pt.paradigmshift.babel.plumtree.messages.IHaveMessage} per tree root is sent.
 * Entries persist — and are therefore re-announced on subsequent ticks — until
 * the peer either grafts the message or acknowledges it with an
 * {@code IGNORED_IHAVE}, which is what keeps the protocol reliable across
 * control-message loss.
 *
 * <p>This periodic-tick lazy-push model goes beyond the paper's per-message
 * timer; it follows Riak's {@code riak_core_broadcast} (originally implemented
 * by Jordan West). See {@link pt.paradigmshift.babel.plumtree.MultiPlumtree}
 * for the full acknowledgement and link.
 *
 * <p>Handler class: timer. <b>ID:</b> {@value #TIMER_CODE}. Owning protocol:
 * {@code MultiPlumtree} (id 3100).
 */
public class LazyTickTimer extends ProtoTimer {

    /** Babel timer numeric identifier. */
    public static final short TIMER_CODE = 3101;

    public LazyTickTimer() {
        super(TIMER_CODE);
    }

    /** Stateless timer: returns {@code this}. */
    @Override
    public ProtoTimer clone() {
        return this;
    }
}
