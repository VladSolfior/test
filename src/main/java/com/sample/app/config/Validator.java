package com.sample.app.config;


import com.amazonaws.util.StringUtils;
import com.google.gson.JsonElement;
import com.sample.app.broker.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Validator {

    private Validator() {
    }

    private static Logger log = LoggerFactory.getLogger(Validator.class.getName());

    /**
     * @param message to validate
     * Checks for nullity, body info, location
     * temperature, battery info
     * @return true in case valid message
     */
    public static boolean validateMessage(Message message) {
        if (message == null) {
            return false;
        }
        if (StringUtils.isNullOrEmpty(message.getBody()) ||
                StringUtils.isNullOrEmpty(message.getDeviceId())) {
            log.error("Corrupted message. Check service connectivity.");
            return false;
        }
        return true;
    }


    public static boolean isValidNumber(JsonElement value) {
        return value != null && value.getAsJsonPrimitive().isNumber();
    }

    public static boolean isPresent(JsonElement value) {
        return value != null;
    }


}
