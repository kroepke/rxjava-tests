
Small excercise to try to implement a per-thread, time and size limited flushing system with RxJava.
Arose out of discussion on Twitter: https://twitter.com/kroepke/status/548136685293568000

Simply run Main.java.

The interesting part is in Main.Processor, namely the thread local subject variable and its use of window()
