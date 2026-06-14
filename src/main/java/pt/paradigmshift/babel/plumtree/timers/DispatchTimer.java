package pt.paradigmshift.babel.plumtree.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

/**
 * Periodic timer that drives the single-tree {@code Plumtree} lazy-push
 * <em>dispatch policy</em>: on each tick the accumulated lazy queue is flushed
 * as batched {@code IHAVE} control messages (the paper's {@code dispatch}
 * procedure). Configuring the period to {@code 0} disables the timer and makes
 * the protocol dispatch each announcement immediately instead.
 *
 * <p>Handler class: timer. <b>ID:</b> {@value #TIMER_CODE}. Owning protocol:
 * {@code Plumtree} (id 3000).
 */
public class DispatchTimer extends ProtoTimer {

    /** Babel timer numeric identifier. */
    public static final short TIMER_CODE = 3002;

    /** Create the periodic lazy-dispatch timer (scheduled with the configured period). */
    public DispatchTimer() {
        super(TIMER_CODE);
    }

    /** Stateless timer: returns {@code this}. */
    @Override
    public ProtoTimer clone() {
        return this;
    }
}
