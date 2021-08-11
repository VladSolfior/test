package com.sample.app.service;


import static com.sample.app.config.Constants.NON_PROCESSED_TOPIC;
import static com.sample.app.config.Constants.NON_VALID_MESSAGE;
import static com.sample.app.config.Constants.ORDERED_TOPIC;
import static com.sample.app.config.Constants.RAW_TOPIC;
import static com.sample.app.config.Constants.SAMPLE_DURATION;
import static com.sample.app.config.Validator.validateMessage;

import com.sample.app.broker.Message;
import com.sample.app.broker.MessageBroker;
import com.sample.app.dao.Dao;

import java.io.IOException;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceImpl implements Service {

    private static Logger log = LoggerFactory.getLogger(ServiceImpl.class.getName());
    private static final Duration DURATION = Duration.ofMillis(SAMPLE_DURATION);

    private Dao dao;
    private MessageBroker broker;

    public ServiceImpl(Dao dao, MessageBroker broker) {
        this.dao = dao;
        this.broker = broker;
    }

    @Override
    public void processMessage() throws NonProcessedException {
        Message message = null;
        try {
            message = broker.poll(RAW_TOPIC, DURATION);
            if (!validateMessage(message)) {
                throw new NonProcessedException(String.format(
                        NON_VALID_MESSAGE,message.getDeliveryId(), RAW_TOPIC
                ));
            }
            log.info("got message, delivery id: {}", message.getDeliveryId());
            short processingId = dao.getProcessingId();
            if (processingId == message.getDeliveryId()) {
                publish(ORDERED_TOPIC, message);
                acknowledge(message);
                short updatedId = dao.incrementProcessingId(processingId);
                Message storedMessage = dao.getMessageByProcessingId(updatedId);

                while (storedMessage != null) {
                    publish(ORDERED_TOPIC, storedMessage);
                    updatedId = dao.incrementProcessingId(processingId);
                    storedMessage = dao.getMessageByProcessingId(updatedId);
                }
            } else {
                dao.writeMessage(message);
                acknowledge(message);
            }
        } catch (Exception e) {
            if (message != null) {
                log.error(
                        "non processed message: {}, cause: {}",
                        message.getDeliveryId(),
                        e.getMessage() + " " + e.getCause());
                if (e.getClass().equals(NonProcessedException.class)) {
                    publish(NON_PROCESSED_TOPIC, message);
                }
            } else {
                log.error("Cannot retrieve messages from broker");
                throw new NonProcessedException("Cannot retrieve messages from broker", e);
            }
        }
    }

    private void publish(String topicName, Message message) throws NonProcessedException {
        try {
            broker.publish(topicName, message);
        } catch (IOException e) {
            log.error(
                    "Cannot process message with id: {}, topic to write: {}",
                    message.getDeliveryId(),
                    topicName);
            throw new NonProcessedException(e);
        }
    }

    private void acknowledge(Message message) throws IOException {
        broker.acknowledge(RAW_TOPIC, message.getPartition(), message.getOffset());
        log.info("Acknowledgment sent, message id: {}", message.getDeliveryId());
    }
}
