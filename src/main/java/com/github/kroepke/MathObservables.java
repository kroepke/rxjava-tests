package com.github.kroepke;

import rx.Observable;
import rx.Subscriber;
import rx.exceptions.Exceptions;
import rx.exceptions.OnErrorThrowable;

public class MathObservables {
    public static Observable<Long> deriveLong(Observable<Long> source) {
        return source.lift(new OperatorDeriveLong());
    }

    public static Observable<Integer> deriveInteger(Observable<Integer> source) {
        return source.lift(new OperatorDeriveInteger());
    }

    public static Observable<Float> deriveFloat(Observable<Float> source) {
        return source.lift(new OperatorDeriveFloat());
    }

    public static Observable<Double> deriveDouble(Observable<Double> source) {
        return source.lift(new OperatorDeriveDouble());
    }

    public static class OperatorDeriveInteger implements Observable.Operator<Integer, Integer> {
        @Override
        public Subscriber<? super Integer> call(Subscriber<? super Integer> subscriber) {
            return new Subscriber<Integer>(subscriber) {
                private Integer value;

                @SuppressWarnings("unchecked")
                @Override
                public void onNext(Integer currentValue) {
                    if (this.value != null) {
                        try {
                            this.value = currentValue - value;
                        } catch (Throwable e) {
                            Exceptions.throwIfFatal(e);
                            subscriber.onError(OnErrorThrowable.addValueAsLastCause(e, currentValue));
                            return;
                        }
                        subscriber.onNext(this.value);
                    } else {
                        subscriber.onNext(null);
                    }
                    this.value = currentValue;
                }

                @Override
                public void onError(Throwable e) {
                    subscriber.onError(e);
                }

                @Override
                public void onCompleted() {
                    subscriber.onCompleted();
                }
            };

        }
    }

    public static class OperatorDeriveLong implements Observable.Operator<Long, Long> {
        @Override
        public Subscriber<? super Long> call(Subscriber<? super Long> subscriber) {
            return new Subscriber<Long>(subscriber) {
                private Long value;

                @SuppressWarnings("unchecked")
                @Override
                public void onNext(Long currentValue) {
                    if (this.value != null) {
                        try {
                            this.value = currentValue - value;
                        } catch (Throwable e) {
                            Exceptions.throwIfFatal(e);
                            subscriber.onError(OnErrorThrowable.addValueAsLastCause(e, currentValue));
                            return;
                        }
                        subscriber.onNext(this.value);
                    } else {
                        subscriber.onNext(null);
                    }
                    this.value = currentValue;
                }

                @Override
                public void onError(Throwable e) {
                    subscriber.onError(e);
                }

                @Override
                public void onCompleted() {
                    subscriber.onCompleted();
                }
            };

        }
    }

    public static class OperatorDeriveFloat implements Observable.Operator<Float, Float> {
        @Override
        public Subscriber<? super Float> call(Subscriber<? super Float> subscriber) {
            return new Subscriber<Float>(subscriber) {
                private Float value;

                @SuppressWarnings("unchecked")
                @Override
                public void onNext(Float currentValue) {
                    if (this.value != null) {
                        try {
                            this.value = currentValue - value;
                        } catch (Throwable e) {
                            Exceptions.throwIfFatal(e);
                            subscriber.onError(OnErrorThrowable.addValueAsLastCause(e, currentValue));
                            return;
                        }
                        subscriber.onNext(this.value);
                    } else {
                        subscriber.onNext(null);
                    }
                    this.value = currentValue;
                }

                @Override
                public void onError(Throwable e) {
                    subscriber.onError(e);
                }

                @Override
                public void onCompleted() {
                    subscriber.onCompleted();
                }
            };

        }
    }

    public static class OperatorDeriveDouble implements Observable.Operator<Double, Double> {
        @Override
        public Subscriber<? super Double> call(Subscriber<? super Double> subscriber) {
            return new Subscriber<Double>(subscriber) {
                private Double value;

                @SuppressWarnings("unchecked")
                @Override
                public void onNext(Double currentValue) {
                    if (this.value != null) {
                        try {
                            this.value = currentValue - value;
                        } catch (Throwable e) {
                            Exceptions.throwIfFatal(e);
                            subscriber.onError(OnErrorThrowable.addValueAsLastCause(e, currentValue));
                            return;
                        }
                        subscriber.onNext(this.value);
                    } else {
                        subscriber.onNext(null);
                    }
                    this.value = currentValue;
                }

                @Override
                public void onError(Throwable e) {
                    subscriber.onError(e);
                }

                @Override
                public void onCompleted() {
                    subscriber.onCompleted();
                }
            };

        }
    }
}
