package com.kafkalearn.exception;

import com.kafkalearn.api.dto.CommandResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * 全局异常处理器。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * 处理账户不存在。
     *
     * @param ex 异常
     * @return 404 响应
     */
    @ExceptionHandler(AccountNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNotFound(AccountNotFoundException ex) {
        return build(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    /**
     * 处理余额不足。
     *
     * @param ex 异常
     * @return 422 响应
     */
    @ExceptionHandler(InsufficientBalanceException.class)
    public ResponseEntity<Map<String, Object>> handleInsufficientBalance(InsufficientBalanceException ex) {
        return build(HttpStatus.UNPROCESSABLE_ENTITY, ex.getMessage());
    }

    /**
     * 处理账户已关闭。
     *
     * @param ex 异常
     * @return 409 响应
     */
    @ExceptionHandler(AccountClosedException.class)
    public ResponseEntity<Map<String, Object>> handleAccountClosed(AccountClosedException ex) {
        return build(HttpStatus.CONFLICT, ex.getMessage());
    }

    /**
     * 处理乐观锁冲突。
     *
     * @param ex 异常
     * @return 409 响应
     */
    @ExceptionHandler(OptimisticLockConflictException.class)
    public ResponseEntity<Map<String, Object>> handleOptimisticLock(OptimisticLockConflictException ex) {
        return build(HttpStatus.CONFLICT, ex.getMessage());
    }

    /**
     * 处理命令处理中。
     *
     * @param ex 异常
     * @return 409 响应
     */
    @ExceptionHandler(CommandInProgressException.class)
    public ResponseEntity<Map<String, Object>> handleInProgress(CommandInProgressException ex) {
        return build(HttpStatus.CONFLICT, ex.getMessage());
    }

    /**
     * 处理幂等重复命令（返回首次成功结果）。
     *
     * @param ex 异常
     * @return 200 响应
     */
    @ExceptionHandler(DuplicateCommandException.class)
    public ResponseEntity<CommandResponse> handleDuplicate(DuplicateCommandException ex) {
        var record = ex.getExistingRecord();
        return ResponseEntity.ok(new CommandResponse(
                record.getAggregateId(),
                record.getResultEventId(),
                0L,
                true
        ));
    }

    /**
     * 处理参数校验失败。
     *
     * @param ex 异常
     * @return 400 响应
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> fields = new HashMap<>();
        for (FieldError error : ex.getBindingResult().getFieldErrors()) {
            fields.put(error.getField(), error.getDefaultMessage());
        }
        Map<String, Object> body = new HashMap<>();
        body.put("timestamp", Instant.now());
        body.put("status", HttpStatus.BAD_REQUEST.value());
        body.put("message", "参数校验失败");
        body.put("fields", fields);
        return ResponseEntity.badRequest().body(body);
    }

    /**
     * 处理非法参数。
     *
     * @param ex 异常
     * @return 400 响应
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException ex) {
        return build(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    private ResponseEntity<Map<String, Object>> build(HttpStatus status, String message) {
        Map<String, Object> body = new HashMap<>();
        body.put("timestamp", Instant.now());
        body.put("status", status.value());
        body.put("message", message);
        return ResponseEntity.status(status).body(body);
    }
}
