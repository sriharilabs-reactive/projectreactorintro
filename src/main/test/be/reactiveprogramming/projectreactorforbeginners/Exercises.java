package be.reactiveprogramming.projectreactorforbeginners;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class Exercises {

  /**
   * TODO 00 READ: Welcome to Project Reactor Intro. We'll be doing a number exercises to get acquainted with this Reactive Streams library.
   * The exercises are laid out in the form of tests to be able to run them easily. Most tests will verify your code using the StepVerifier.
   * In some tests you'll have to verify manually.
   *
   * If you import this project into your favorite IDE (IntelliJ), don't forget to mark the test folder as your 'Test Sources Root'.
   */

  /**
   * TODO 01 READ: Reactive Programming is all about data Pipelines through which your data flows. The programming model works around these 4 concepts:
   *
   * Producers
   *    Emit the data (source of your data pipeline)
   * Subscribers
   *    Receive the data (tail of your data pipeline)
   * Subscription
   *    When you subscribe to a producer you get a subscription (comparable to a session). This subscription contains the context information and
   *    also allows the subscriber to control the receiving data stream (ex. backpressure)
   * Processors
   *    Are both subscribers and producer. They subscribe to the datastream upstream, do an operation on the data and emit the data downstream.
   *    Ex. a filter or a mapper
   *
   * In Project Reactor we can distinguish 2 types of Producers:
   *    Flux
   *      This is the most common Publisher but one of the most versatile at the same time.
   *      It will emit 0, 1, or any number of signals until it ends with either an "onComplete" or "onError" signal
   *
   *    Mono
   *      This producer will emit either 0 or 1 value, or emits an "onError" signal
   *
   * Reactive programming builds on top of functional programming and therefore relies on the concepts of:
   *    Functional composition
   *      The API is really nice allowing you to use a builder pattern style to compose your data pipeline in a very readable manner
   *    Lazy evaluation
   *      Before the Flux does anything, it needs to be subscribed to using the subscribe method. This method can have an optional Consumer parameter, this Consumer
   *      will process the information that Streams to the end of the pipeline.
   *
   * Usually the "source" Fluxes in an application will be supplied from external resources that are Producers of these signals, like a messaging system,
   * database resultset, a filehandle, etc. For simplicity's sake however, these exercises will be created with some basic values that will be provided
   * in the Flux itself.
   *
   * The exercises will explain the result you should try to achieve, which you can visualize by printing to your Console. Reactive Pipelines work in a
   * non-blocking way, so you'll often be required to use the sleep() method below to keep the main thread running.
   */

  public void sleep(int milliSeconds) {
    try {
      new CountDownLatch(1).await(milliSeconds, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
    }
  }

  /**
   * TODO 02 Create your first Reactive Pipeline of Publishers and Subscribers from this initial Flux. Apply a filter to remove the uneven numbers
   * and then map the numbers by multiplying them by two.
   *
   * Don't forget to subscribe to the Reactive Stream with a Consumer that prints to the command line.
   * Can you even pull out this Consumer to reuse it between exercises?
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
   * TODO 03 A reactive data pipeline looks a lot like a Java stream but don't be mislead because there are some huge differences as how
   * they work underneath as well as the possibility to get notified of all kinds of events.
   *
   * The tree most important events:
   *
   *    doOnNext
   *      Emission of a new data-element
   *
   *    doOnError
   *      Error termination (the data pipeline is closed)
   *
   *    Flux#doOnComplete, Mono#doOnSuccess
   *      The end of the data stream
   *
   * In project reactor there are a lot more events like doOnEach, ... If you're interested, take a look here :
   * "https://projectreactor.io/docs/core/release/reference/#which.peeking
   *
   * Use the doOnComplete method to print out "Completed!" when you receive the complete signal.
   */
  @Test
  public void onCompleteTest() {
    Flux.range(1, 10)
        .doOnComplete(() -> System.out.println("Completed!"))
        .subscribe(commandLinePrinter);

    sleep(2000);
  }

  /**
   * TODO 04 The reactive manifesto states resilience as one of the four concepts, meaning that we want to treat Errors as first class citizens.
   *
   * In case an exception happens, somewhere in your data-pipeline, it will send an "onError" signal down the stream so you can act on it.
   * Also note that when this signal is sent the data-pipeline is automatically ended. It (probably) wouldn't make sense to send anymore data
   * resulting in more errors but you have full control how you want to handle the error (more on that in the exercise below).
   *
   * Your task here is to cause an exception inside the map method causing an error signal to be send.
   * Next, return the default value "0" in case this happens (look at all the different onError methods)
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
   * TODO 05 Different Fluxes can be combined into one through different methods. Take a look at the article below to see some examples:
   * https://www.reactiveprogramming.be/project-reactor-combining-fluxes/
   *
   * Give the zipWith method a try to combine the following two Fluxes into one. The test will end with a StepVerifier.
   * This is a mechanism supplied by Project Reactor to easily test your Reactive Pipelines.
   */
  @Test
  public void sharingTheInput() {
    Flux<String> letters = Flux.just("a", "b", "c", "d", "e");
    Flux<Integer> numbers = Flux.range(1, 5);

    Flux<String> lettersAndNumbers = Flux.zip(letters, numbers).map(t -> t.getT1() + t.getT2());

    StepVerifier.create(lettersAndNumbers).expectNext("a1", "b2", "c3", "d4", "e5").verifyComplete();
  }

}