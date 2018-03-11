package de.iani.cubesidestats.api;

public interface Callback<T> {
    public void call(T data);
}
