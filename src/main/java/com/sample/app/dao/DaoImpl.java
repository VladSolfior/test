package com.sample.app.dao;


import com.sample.app.broker.Message;

import java.util.concurrent.atomic.AtomicInteger;

public class DaoImpl implements Dao {

    /**
     * Test only implementation.
     * should be removed after done dao layer.
     */
    private AtomicInteger counter;

    public DaoImpl() {
        this.counter = new AtomicInteger(1);

    }

    @Override
    public short getProcessingId() {
        return counter.shortValue();
    }

    @Override
    public short incrementProcessingId(short id) {
        int i = counter.incrementAndGet();
        if (i >= Short.MAX_VALUE) {
            this.counter = new AtomicInteger(1);
        }
        return counter.shortValue();
    }

    @Override
    public void writeMessage(Message message) {
    	//tmp to do write implementation
    }

    @Override
    public Message getMessageByProcessingId(short updatedId) {
        return null;
    }
}
