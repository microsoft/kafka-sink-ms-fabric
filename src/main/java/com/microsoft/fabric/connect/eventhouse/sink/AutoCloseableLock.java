package com.microsoft.fabric.connect.eventhouse.sink;

import java.util.concurrent.locks.Lock;

import org.jetbrains.annotations.NotNull;

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
