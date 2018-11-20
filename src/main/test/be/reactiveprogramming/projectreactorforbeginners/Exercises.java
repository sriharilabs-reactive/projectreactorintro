package be.reactiveprogramming.projectreactorforbeginners;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class Exercises {

  /**
   * TODO 00 READ: Welcome to Project Reactor Intro. We'll be doing a number exercises to get acquainted with this Reactive Streams library. The exercises are laid out in the form of tests
   * to be able to run them easily. These tests will try to verify your results as well as possible through the StepVerifier, but often you'll also have to check manually if they worked as inspected.
   */

  /**
   * TODO 01 READ: Reactive Programming consists out of Reactive Pipelines formed by Producers sending signals with data to Subscribers. Often these Subscribers are message Producers as well and will
   * send the signals further on to other Subscribers after filtering or mapping them to new values.
   *
   * The Flux is the most common Publisher in Project Reactor, but one of the most versatile at the same time. It will emit 0, 1, or any number of signals to its Subscriber, until it ends with either
   * an "onComplete" or "onError" signal. There's also a more basic version of the Flux called the Mono, which will emit either 0 or 1 value, or emits an "onError" signal.
   *
   * Both Fluxes and Monos form a part of a Reactive Pipeline. These pipelines can be extended with new Producers and Subscribers through a builder pattern. Before the Flux does anything, it needs to
   * be subscribed to using the subscribe method. This method can have an optional Consumer parameter, this Consumer will process the information that Streams to the end of the pipeline.
   *
   * Usually the "source" Fluxes in an application will be supplied from external resources that are Producers of these signals, like a messaging system, database resultset, a filehandle, etc. For
   * simplicity's sake however, these exercises will be created with some basic values that will be provided in the Flux itself.
   *
   * The exercises will explain the result you should try to achieve, which you can visualize by printing to your Console. Reactive Pipelines work in a non-blocking way, so you'll be required to use
   * the sleep() method underneath this text to make sure the thread doesn't stop by itself.
   */

  public void sleep(int milliSeconds) {
    try {
      new CountDownLatch(1).await(milliSeconds, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
    }
  }

  /**
   * TODO 02 Create your first Reactive Pipeline of Publishers and Subscribers from this initial Flux. Apply a filter to remove the uneven numbers, and then map the numbers by multiplying them by two.
   * Don't forget to subscribe to the Reactive Stream with a Consumer that prints to the command line. Can you even pull out this Consumer to reuse it between exercises?
   *
   * We expect to see the numbers "4, 8, 12, 16, 20" printed on the command line.
   */

  private Consumer<Integer> commandLinePrinter = o -> System.out.println(o);

  @Test
  public void publisherTest() {
    Flux.range(1, 10)
        .filter(n -> n % 2 == 0)
        .map(n -> n * 2)
        .subscribe(commandLinePrinter);

    sleep(2000);
  }

  /**
   * TODO 03 We can see a bit more information of what goes on in Project Reactor by using methods like "doEach" and "doOnComplete" on a Flux. These will trigger the run method on their Consumer or
   * Runnable parameter the moment a new value, or an "onComplete" gets signaled. Use this method to print out "complete" when you receive the complete signal.
   */
  @Test
  public void onCompleteTest() {
    Flux.range(1, 10)
        .doOnComplete(() -> System.out.println("Completed!"))
        .subscribe(commandLinePrinter);

    sleep(2000);
  }

  /**
   * TODO 04 Errors are first class citizens in Project Reactor. In case an exception happens in a Producer, it will send an "onError" signal, so an interaction can happen based on it. When this
   * signal is sent, the Producer is automatically ended. Try to cause an exception to happen inside a map method, so the created Flux will send an error signal. Try to chain on a method that will
   * return the default value "0" in case this happens.
   */
  @Test
  public void errorHandling() {
    Flux.just(5)
        .map(n -> n / 0)
        .onErrorReturn(0)
        .subscribe(commandLinePrinter);

    sleep(2000);
  }

  /**
   * TODO 05 Different Fluxes can be combined into one through different methods. Take a look at the article https://www.reactiveprogramming.be/project-reactor-combining-fluxes/ to see a couple of
   * ways to combine different Fluxes. Give the zipWith method a try to combine the following two Fluxes into one. The test will end with a StepVerifier. This is a mechanism supplied by Project
   * Reactor to easily test your Reactive Pipelines.
   */
  @Test
  public void sharingTheInput() {
    Flux<String> letters = Flux.just("a", "b", "c", "d", "e");
    Flux<Integer> numbers = Flux.range(1, 5);

    Flux<String> lettersAndNumbers = Flux.zip(letters, numbers).map(t -> t.getT1() + t.getT2());

    StepVerifier.create(lettersAndNumbers).expectNext("a1", "b2", "c3", "d4", "e5").verifyComplete();
  }

}
