package com.sample.app.dao;


import com.sample.app.broker.Message;

public interface Dao {

    short getProcessingId();

    short incrementProcessingId(short id);

    boolean writeMessage(Message message);

    Message getMessageByProcessingId(short updatedId);
}
