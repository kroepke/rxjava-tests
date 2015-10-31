package com.github.kroepke;

import com.google.common.collect.Maps;

import java.util.Map;

public class Message {

    private Map<String, Object> fields = Maps.newTreeMap();

    public Message() {
    }

    public Message(Message other) {
        fields.putAll(other.getFields());
    }

    public Object getField(String fieldName) {
        return fields.get(fieldName);
    }

    public Message setField(String fieldName, Object value) {
        fields.put(fieldName, value);
        return this;
    }

    @Override
    public String toString() {
        return "Message{" +
                "fields=" + fields +
                '}';
    }

    public Map<String, Object> getFields() {
        return fields;
    }
}
