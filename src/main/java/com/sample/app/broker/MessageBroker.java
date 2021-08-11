package com.sample.app.broker;

import java.io.IOException;
import java.time.Duration;

/**
 * An approximation of an actual MessageBroker
 */
public interface MessageBroker {

    /**
     * Polls for the next message available in the topic of given name.
     * If there are no messages available, the method will block until
     * message becomes available or until timeout is reached, whichever
     * comes first. If timeout is zero, the method returns without
     * blocking.
     *
     * The consumed messages should be acknowledged once they were
     * successfully processed.
     *
     * @param topicName Name of topic to poll.
     * @param timeout Time to wait for next message in case it is not
     *        immediately available.
     *
     * @return Message or null, if there was no message available and
     *         timeout expired.
     *
     * @throws IOException When it wasn't possible to communicate with
     *         the broker.
     */
    Message poll(String topicName, Duration timeout) throws IOException;


    /**
     * Marks message with given ID as processed, so that it won't be
     * made visible to other topic consumers ore reprocessed. If message won't be
     * marked as processed before timeout expires, the message will become
     * available for consumption for other consumers.
     *
     * @param topicName The topic of the message.
     * @param partition The partition the message was read from.
     * @param offset The offset of the message within the partition.
     * @throws IOException When it wasn't possible to communicate with
     *      *         the broker.
     *
     * @see Message#getPartition()
     * @see Message#getOffset()
     */
    void acknowledge(String topicName, int partition, long offset) throws IOException;

    /**
     * Publishes the message to another topic.
     *
     * @param topicName The name of the other topic.
     * @param message The message to publish.
     * @throws IOException If there were any problems. The message might not have been published.
     */
    void publish(String topicName, Message message) throws IOException;
}
