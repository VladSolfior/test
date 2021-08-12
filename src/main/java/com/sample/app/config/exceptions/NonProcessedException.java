package com.sample.app.config.exceptions;

import java.io.IOException;

public class NonProcessedException extends IOException {

    public NonProcessedException(String message, Throwable cause) {
        super(message, cause);
    }

    public NonProcessedException(Throwable cause) {
        super(cause);
    }

    public NonProcessedException(String message) {
        super(message);
    }
}
