package com.github.kroepke;

import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;

import java.util.Date;

import static com.github.kroepke.MathObservables.deriveInteger;

public class ProjectionCollectionTest {

    private Observable<Message> messages;

    @Before
    public void setup() {
        final long startTime = new Date().getTime();

        messages = Observable.from(new Message[]{
                new Message().setField("timestamp", new Date(startTime))
                        .setField("source", "a")
                        .setField("cpu.util", 15)
                        .setField("mem.avail", 94),
                new Message().setField("timestamp", new Date(startTime + 5 * 1000))
                        .setField("source", "b")
                        .setField("cpu.util", 10)
                        .setField("mem.avail", 98),
                new Message().setField("timestamp", new Date(startTime + 10 * 1000))
                        .setField("source", "a")
                        .setField("cpu.util", 24)
                        .setField("mem.avail", 79),
                new Message().setField("timestamp", new Date(startTime + 15 * 1000))
                        .setField("source", "b")
                        .setField("cpu.util", 18)
                        .setField("mem.avail", 50),
                new Message().setField("timestamp", new Date(startTime + 20 * 1000))
                        .setField("source", "a")
                        .setField("cpu.util", 12)
                        .setField("mem.avail", 96),
                new Message().setField("timestamp", new Date(startTime + 25 * 1000))
                        .setField("source", "b")
                        .setField("cpu.util", 4)
                        .setField("mem.avail", 70),
        });
    }

    private Action1<Message> printMessage() {
        return message -> System.out.printf("%s\n", message.toString());
    }

    @Test
    public void filterMultiple() {
        final Observable<Message> sourceA = messages.filter(message -> message.getField("source").equals("a"));
        final Observable<Message> sourceB = messages.filter(message -> message.getField("source").equals("b"));

        Observable.concat(sourceA, sourceB)
                .subscribe(printMessage());
    }

    @Test
    public void projection() {
        final Observable<Integer> cpuUtilDerivative = deriveInteger(project(messages,
                                                                            "cpu.util",
                                                                            Integer.class));
        messages
                .zipWith(cpuUtilDerivative, (message, value) -> message.setField("cpu.util.derive", value))
                .subscribe(printMessage());

    }

    private <R> Observable<R> project(Observable<Message> messages, String fieldName, Class<R> typeClass) {
        return messages
                .map(message -> message.getField(fieldName))
                .cast(typeClass);
    }

}