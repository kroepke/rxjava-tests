/*
 * Copyright 2014 TORCH GmbH
 *
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.github.kroepke;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.subjects.PublishSubject;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.*;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final AtomicLong producerBlocks = new AtomicLong(0);

    public static void main(String[] args) {

        final LinkedBlockingQueue<Long> queue = new LinkedBlockingQueue<>(100);

        Processor processor = new Processor();

        AbstractExecutionThreadService consumer = new Consumer(1, processor, queue);
        AbstractExecutionThreadService consumer1 = new Consumer(2, processor, queue);
        AbstractExecutionThreadService consumer2 = new Consumer(3, processor, queue);

        AbstractExecutionThreadService producer = new Producer(queue);
        AbstractExecutionThreadService waterMarkPrinter = new AbstractExecutionThreadService() {
            @Override
            protected void run() throws Exception {
                while (isRunning()) {
                    sleepUninterruptibly(1, SECONDS);
                    log.info("Queue watermark {}, producer blocks {}", queue.size(), producerBlocks.get());
                }
            }
        };

        waterMarkPrinter.startAsync().awaitRunning();
        consumer.startAsync().awaitRunning();
        consumer1.startAsync().awaitRunning();
        consumer2.startAsync().awaitRunning();
        producer.startAsync().awaitRunning();

        sleepUninterruptibly(1, MINUTES);

        producer.stopAsync().awaitTerminated();
        consumer.stopAsync().awaitTerminated();
        consumer1.stopAsync().awaitTerminated();
        consumer2.stopAsync().awaitTerminated();
        waterMarkPrinter.stopAsync().awaitTerminated();

    }

    private static class Producer extends AbstractExecutionThreadService {
        private static final Logger log = LoggerFactory.getLogger(Producer.class);
        private final LinkedBlockingQueue<Long> queue;

        public Producer(LinkedBlockingQueue<Long> queue) {
            this.queue = queue;
        }

        @Override
        protected void run() throws Exception {

            long i = 0;
            while (isRunning()) {
                long elem = i++;
                boolean successful = queue.offer(elem);
                if (!successful) {
                    producerBlocks.incrementAndGet();
                    queue.put(elem);
                }
                // sometimes stall the producer so that flushes based on time will happen in the consumers
                if (i % 100000 == 0) {
                    log.info("Stalling producer for 2 seconds");
                    sleepUninterruptibly(2, SECONDS);
                }
            }
        }
    }

    private static class Consumer extends AbstractExecutionThreadService {
        private int threadNumber;
        private Processor processor;
        private final LinkedBlockingQueue<Long> queue;

        public Consumer(int i, Processor processor, LinkedBlockingQueue<Long> queue) {
            threadNumber = i;
            this.processor = processor;
            this.queue = queue;
        }

        @Override
        protected String serviceName() {
            return super.serviceName() + threadNumber;
        }

        @Override
        protected void run() throws Exception {

            while (isRunning()) {
                Long aLong = queue.take();
                processor.process(aLong);
            }
        }
    }

    private static class Processor {
        private static final Logger log = LoggerFactory.getLogger(Processor.class);
        private final FlushSubscriber flushSubscriber = new FlushSubscriber();
        private final ThreadLocal<PublishSubject<Long>> subject = new ThreadLocal<PublishSubject<Long>>() {
            @Override
            protected PublishSubject<Long> initialValue() {
                PublishSubject<Long> subject1 = PublishSubject.create();
                subject1.window(1, SECONDS, 19).subscribe(new Action1<Observable<Long>>() {
                    @Override
                    public void call(Observable<Long> longObservable) {
                        longObservable.toList().forEach(new Action1<List<Long>>() {
                            @Override
                            public void call(List<Long> longs) {
                                flushSubscriber.onNext(longs);
                            }
                        });
                    }
                });
                return subject1;
            }
        };
        private static final AtomicInteger concurrentFlushes = new AtomicInteger();

        private void flush(List<Long> longs) {
            try {
                int concurrentFlushes = Processor.concurrentFlushes.incrementAndGet();
                if (longs.size() == 20) {
                    log.info("Flushing {} items because window size was reached, concurrent flushes {}",
                             longs.size(),
                             concurrentFlushes);
                } else {
                    log.info("Flushing {} items because window timeout was reached. Concurrent flushes {}",
                             longs.size(),
                             concurrentFlushes);
                    if (longs.size() == 0) {
                        // don't simulate slow flush for zero items
                        return;
                    }
                }
                sleepUninterruptibly(200, MILLISECONDS);
            } finally {
                concurrentFlushes.getAndDecrement();
            }
        }

        public void process(Long aLong) {
            subject.get().onNext(aLong);
        }

        private class FlushSubscriber extends Subscriber<List<Long>> {
            @Override
            public void onCompleted() {
                log.info("completed, no more items coming.");
            }

            @Override
            public void onError(Throwable e) {
                log.error("Exception caught", e);
            }

            @Override
            public void onNext(List<Long> longs) {
                flush(longs);
            }
        }
    }
}
