package com.drencak.push.server;

public class Message {
    public final String key;
    public final String props;
    public final String value;

    public Message(String key, String props, String value) {
        this.key = key;
        this.props = props;
        this.value = value;
    }
}
