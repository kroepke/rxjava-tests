package com.github.kroepke;

import rx.Observable;

import static rx.Observable.range;

public class Derivative {
    public static void main(String[] args) {

        final Observable<Integer> numbers = range(1, 10);

        MathObservables.deriveInteger(numbers.map(aLong -> aLong * 2))
                .subscribe(integer -> System.out.printf("%d ", integer));
    }
}
