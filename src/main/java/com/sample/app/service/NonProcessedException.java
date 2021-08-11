package com.sample.app.service;

import java.io.IOException;

class NonProcessedException extends IOException {

    NonProcessedException(String message, Throwable cause) {
        super(message, cause);
    }

    NonProcessedException(Throwable cause) {
        super(cause);
    }

    NonProcessedException(String message) {
        super(message);
    }
}
