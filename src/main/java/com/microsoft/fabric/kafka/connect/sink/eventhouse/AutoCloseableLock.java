package com.microsoft.fabric.kafka.connect.sink.eventhouse;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.locks.Lock;

public class AutoCloseableLock implements AutoCloseable {
    private final Lock lock;

    public AutoCloseableLock(@NotNull Lock lock) {
        this.lock = lock;
        lock.lock();
    }

    @Override
    public void close() {
        lock.unlock();
    }
}
