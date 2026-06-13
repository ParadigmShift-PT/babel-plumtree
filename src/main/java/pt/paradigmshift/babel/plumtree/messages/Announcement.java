package pt.paradigmshift.babel.plumtree.messages;

import java.util.UUID;

/**
 * A single lazy-push announcement: the identifier of a message a peer claims
 * to have, together with the {@code round} (hop count from the originator) at
 * which it received it.
 *
 * <p>Carried in batches by {@link IHaveMessage} (and acknowledged by
 * {@link IgnoredIHaveMessage}). The {@code round} lets the receiver reason
 * about path length — both to populate the {@code missing} history that a
 * later {@link GraftMessage} draws on, and (in the single-tree {@code Plumtree}
 * tree-optimization) to compare the lazy path length against the eager one.
 *
 * @param messageId the announced message's UUID
 * @param round     the hop count at which the announcer received the message
 */
public record Announcement(UUID messageId, int round) {
}
