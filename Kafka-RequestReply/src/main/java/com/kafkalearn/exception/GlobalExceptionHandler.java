package com.kafkalearn.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/13 11:47
 * @Description:
 */

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ReplyTimeoutException.class)
    public ResponseEntity<Map<String, Object>> handleTimeout(ReplyTimeoutException ex) {
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                .body(Map.of(
                        "status", "TIMEOUT",
                        "message", ex.getMessage()
                ));
    }
}
