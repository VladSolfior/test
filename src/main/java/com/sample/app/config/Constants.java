package com.sample.app.config;


public class Constants {

    private Constants() {
    }

    public static final long SAMPLE_DURATION = 100L;


    public static final String RAW_TOPIC = "raw_device_messages";
    public static final String ORDERED_TOPIC = "ordered_device_messages";
    public static final String NON_PROCESSED_TOPIC = "non_processed_messages";
}
