package com.sample.app.service;


import static com.sample.app.config.Constants.ORDERED_TOPIC;
import static com.sample.app.config.Constants.RAW_TOPIC;

import com.sample.app.broker.Message;
import com.sample.app.broker.MessageBroker;
import com.sample.app.dao.Dao;

import java.io.IOException;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceImpl implements Service {

    private static final Duration DURATION = Duration.ofMillis(100);
    private static Logger log = LoggerFactory.getLogger(ServiceImpl.class.getName());


    private Dao dao;
    private MessageBroker broker;

    public ServiceImpl(Dao dao, MessageBroker broker) {
        this.dao = dao;
        this.broker = broker;
    }

    @Override
    public void processMessage() throws IOException {
        Message message = broker.poll(RAW_TOPIC, DURATION);
        log.info("got message, delivery id: {}", message.getDeliveryId());
        short processingId = dao.getProcessingId();
        boolean processed = false;
        try {
            if (processingId == message.getDeliveryId()) {
                broker.publish(ORDERED_TOPIC, message);
                short updatedId = dao.incrementProcessingId(processingId);
                processed = true;
                Message storedMessage = dao.getMessageByProcessingId(updatedId);

                while (storedMessage != null) {
                    processed = false;
                    broker.publish(ORDERED_TOPIC, storedMessage);
                    updatedId = dao.incrementProcessingId(processingId);
                    processed = true;
                    storedMessage = dao.getMessageByProcessingId(updatedId);
                }

            } else {
                processed = dao.writeMessage(message);
            }
        } catch (Exception e) {
            log.error(
                    "non processed message: {}, cause: {}",
                    message.getDeliveryId(),
                    e.getMessage() + " " + e.getCause());
        }
        if (processed) {
            broker.acknowledge(RAW_TOPIC,message.getPartition(),message.getOffset());
            log.info("Acknowledgment sent, message id: {}", message.getDeliveryId());
        }
    }
}
