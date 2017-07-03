package de.iani.cubesidestats;

public class TimestampedValue<T> {
    private long timestamp;
    private T value;

    public TimestampedValue(T value) {
        this.value = value;
        this.timestamp = System.nanoTime();
    }

    public T get() {
        this.timestamp = System.nanoTime();
        return this.value;
    }

    public void set(T value) {
        this.value = value;
        this.timestamp = System.nanoTime();
    }

    public long getTimestamp() {
        return timestamp;
    }
}
