package com.github.kroepke;

import rx.Observable;
import rx.functions.Action1;

import java.util.Date;

import static rx.Observable.just;

public class ProjectionCollection {
    public static void main(String[] args) {
        final long startTime = new Date().getTime();

        Message msgs[] = new Message[]{
                new Message().setField("timestamp", new Date(startTime))
                        .setField("source", "a")
                        .setField("cpu.util", 15)
                        .setField("mem.avail", 94),
                new Message().setField("timestamp", new Date(startTime + 5 * 1000))
                        .setField("source", "a")
                        .setField("cpu.util", 10)
                        .setField("mem.avail", 98),
                new Message().setField("timestamp", new Date(startTime + 10 * 1000))
                        .setField("source", "a")
                        .setField("cpu.util", 24)
                        .setField("mem.avail", 79),
                new Message().setField("timestamp", new Date(startTime + 15 * 1000))
                        .setField("source", "a")
                        .setField("cpu.util", 18)
                        .setField("mem.avail", 50),
                new Message().setField("timestamp", new Date(startTime + 20 * 1000))
                        .setField("source", "a")
                        .setField("cpu.util", 12)
                        .setField("mem.avail", 96),
                new Message().setField("timestamp", new Date(startTime + 25 * 1000))
                        .setField("source", "a")
                        .setField("cpu.util", 4)
                        .setField("mem.avail", 70),
        };

        final Observable<Message> messages = Observable.from(msgs).share();

        messages.flatMap(message -> just(new LongField(message, "cpu.util")));

        messages.subscribe(printMessages());

    }

    private static Action1<Object> printMessages() {
        return integer -> System.out.printf("%s\n", integer.toString());
    }

    private static class LongField {
        private final Message message;
        private final String name;

        public LongField(Message message, String name) {
            this.message = message;
            this.name = name;
        }
    }
}
