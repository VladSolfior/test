package com.sample.app;

import static com.sample.app.config.Constants.ORDERED_TOPIC;
import static com.sample.app.config.Constants.RAW_TOPIC;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.sample.app.broker.Message;
import com.sample.app.broker.MessageBroker;
import com.sample.app.dao.Dao;
import com.sample.app.service.ServiceImpl;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServiceTest {


    private static final Duration SAMPLE_DURATION = Duration.ofMillis(100);
    private static final int VALID_TO_SEND_INDEX = 1;

    @Mock
    private Dao dao;

    @Mock
    private MessageBroker messageBroker;

    @InjectMocks
    private ServiceImpl service;

    @After
    public void after() {
        verifyNoMoreInteractions(dao, messageBroker);
    }

    @Test
    public void testServiceProcessMessageWritingToDatabase() throws IOException {
        // given
        Message sampleMessage = buildSampleMessage(0);
        Mockito.when(messageBroker.poll(Mockito.anyString(), eq(SAMPLE_DURATION))).thenReturn(sampleMessage);
        Mockito.when(dao.getProcessingId()).thenReturn((short)1);
        // when
        service.processMessage();
        //then
        verify(dao).getProcessingId();
        verify(messageBroker).poll(RAW_TOPIC, SAMPLE_DURATION);
        verify(dao).writeMessage(sampleMessage);
    }

    @Test
    public void testServiceProcessMessagePublishingToTopic() throws IOException {
        //given
        Message sampleMessage = buildSampleMessage(VALID_TO_SEND_INDEX);
        Mockito.when(messageBroker.poll(Mockito.anyString(), eq(SAMPLE_DURATION))).thenReturn(sampleMessage);
        Mockito.when(dao.getProcessingId()).thenReturn((short)VALID_TO_SEND_INDEX);
        short nextDeliveryId = VALID_TO_SEND_INDEX + 1;
        Mockito.when(dao.incrementProcessingId(anyShort())).thenReturn(nextDeliveryId);
        //when
        service.processMessage();
        //then
        verify(dao).getProcessingId();
        verify(dao).incrementProcessingId(sampleMessage.getDeliveryId());
        verify(dao).getMessageByProcessingId(nextDeliveryId);

        verify(messageBroker).poll(RAW_TOPIC, SAMPLE_DURATION);
        verify(messageBroker).publish(ORDERED_TOPIC, sampleMessage);
        verify(messageBroker).acknowledge(RAW_TOPIC, sampleMessage.getPartition(), sampleMessage.getOffset());
    }

    @Test
    public void testServiceProcessMessagesPublishingInLoop() throws IOException {

        //given
        Message sampleMessage = buildSampleMessage(VALID_TO_SEND_INDEX);
        short storedId = VALID_TO_SEND_INDEX + 1;
        Message storedMessage = buildSampleMessage(storedId);

        Mockito.when(messageBroker.poll(Mockito.anyString(), eq(SAMPLE_DURATION)))
                .thenReturn(sampleMessage)
                .thenReturn(storedMessage);
        Mockito.when(dao.getProcessingId()).thenReturn((short)VALID_TO_SEND_INDEX);
        short nextDeliveryId = VALID_TO_SEND_INDEX + 1;
        Mockito.when(dao.incrementProcessingId(anyShort())).thenReturn(nextDeliveryId);

        //when
        service.processMessage();

        //then
        verify(dao).getProcessingId();
        verify(dao).incrementProcessingId(sampleMessage.getDeliveryId());
        verify(dao).getMessageByProcessingId(nextDeliveryId);

        verify(messageBroker).poll(RAW_TOPIC, SAMPLE_DURATION);
        verify(messageBroker).publish(ORDERED_TOPIC, sampleMessage);
        verify(messageBroker).acknowledge(RAW_TOPIC, sampleMessage.getPartition(), sampleMessage.getOffset());
    }

    @Test
    public void shouldProcessMessagesInLoop() throws IOException {
        // given
        List<Message> messages = Arrays.asList(
                buildSampleMessage(1),
                buildSampleMessage(2),
                buildSampleMessage(3));
        Mockito.when(messageBroker.poll(Mockito.anyString(), eq(SAMPLE_DURATION)))
                .thenReturn(messages.get(0))
                .thenReturn(messages.get(1))
                .thenReturn(messages.get(2));

        Mockito.when(dao.getProcessingId())
                .thenReturn((short) VALID_TO_SEND_INDEX)
                .thenReturn((short) 2)
                .thenReturn((short) 3);

        //when
        for (int i = 1; i <= 3; i++) {
            service.processMessage();
        }
        //then
        //pool 3 message
        verify(messageBroker, times(3)).poll(RAW_TOPIC, SAMPLE_DURATION);
        // get processing id 3 times
        verify(dao, times(3)).getProcessingId();

        //increment processing id 3 times
        verify(dao).incrementProcessingId((short) 1);
        verify(dao).incrementProcessingId((short) 2);
        verify(dao).incrementProcessingId((short) 3);
        // check for message in db 3 times
        verify(dao, times(3)).getMessageByProcessingId(anyShort());
        // publish 3 messages
        verify(messageBroker).publish(ORDERED_TOPIC, messages.get(0));
        verify(messageBroker).publish(ORDERED_TOPIC, messages.get(1));
        verify(messageBroker).publish(ORDERED_TOPIC, messages.get(2));

        verify(messageBroker,atLeastOnce()).acknowledge(RAW_TOPIC, messages.get(0).getPartition(), messages.get(0).getOffset());
        verify(messageBroker,atLeastOnce()).acknowledge(RAW_TOPIC, messages.get(1).getPartition(), messages.get(1).getOffset());
        verify(messageBroker,atLeastOnce()).acknowledge(RAW_TOPIC, messages.get(2).getPartition(), messages.get(2).getOffset());
        verify(messageBroker,times(3)).acknowledge(anyString(), anyInt(), anyLong());
    }



    private static Message buildSampleMessage(int deliveryId) {
        short id = (short) deliveryId;
        return new Message(1, 2L, "", id, "Test");
    }
}