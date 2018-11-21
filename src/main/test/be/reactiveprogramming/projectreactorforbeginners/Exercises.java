package be.reactiveprogramming.projectreactorforbeginners;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import reactor.test.StepVerifier;

public class Exercises {

  /**
   * TODO 00 READ: Welcome to Project Reactor Intro. We'll be doing a number exercises to get acquainted with this Reactive Streams library.
   * The exercises are laid out in the form of tests to be able to run them easily. The description of the todos will tell you what's expected.
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

  @Test
  public void publisherTest() {
    Flux.range(1, 10);
    // Let's filter, map, and subscribe

    sleep(2000);
  }

  /**
   * TODO 03 A reactive data pipeline looks a lot like a Java stream but don't be mislead because there are some huge differences as how
   * they work underneath as well as the possibility to get notified of all kinds of events.
   *
   * The three most important events:
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
    Flux.range(1, 10);
    // Let's make clear the Flux completed

    sleep(2000);
  }

  /**
   * TODO 04 The reactive manifesto states resilience as one of the four concepts, meaning that we want to treat Errors as first class citizens.
   *
   * In case an exception happens, somewhere in your data-pipeline, it will send an "onError" signal down the stream so you can act on it.
   * Also note that when this signal is sent the data-pipeline is automatically ended. It (probably) wouldn't make sense to send anymore data
   * resulting in more errors but you have full control how you want to handle the error.
   *
   * Your task here is to cause an exception inside the map method causing an error signal to be send.
   * Next, return the default value "0" in case this happens (look at all the different onError methods)
   */
  @Test
  public void errorHandling() {
    Flux.just(5);
        // Let's give a default value

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

    Flux<String> lettersAndNumbers = null;

    StepVerifier.create(lettersAndNumbers).expectNext("a1", "b2", "c3", "d4", "e5").verifyComplete();
  }

  /**
   * TODO 06 multiple subscribers
   *
   * In contrast to Java streams, a reactive stream can have multiple subscribers and there are different strategies on how you want to handle this.
   * In the test below we have a publisher emitting data every second. Run it and see how it prints a new number every second.
   *
   * Make a second subscription with a delay of 5 seconds in between both subscriptions (HINT: use the sleep() method)
   * Run your test and notice that the second subscriber jumps in after 5 seconds with the first element of the publisher : '0'
   *
   * This is like watching a movie on youtube where a second user wants to start from the beginning (= 2 different subscriptions)
   *
   * Now if we switch to television and the second user turns on his TV after 5 seconds, you would have missed the first 5 seconds and see what everyone
   * else is seeing at that exact moment (= 1 shared subscription)
   *
   * Let's modify the exercise to do just that. We want to change the publisher to share the subscription. Look at the Flux API on how to do that.
   * Run your test again and you should see the second subscription jumping in after 5 seconds getting the same data as the first subscriber
   */
  @Test
  public void multipleSubscribers() {
    Flux<Long> numbers = Flux.interval(Duration.ofSeconds(1)).share();

    numbers.subscribe(process("subscriber 1:"));

    sleep(10000);
  }

  private Consumer<Long> process(String subscriber) {
    return new Consumer<Long>() {
      @Override
      public void accept(Long l) {
        System.out.println(subscriber + " " +l);
      }
    };
  }

  /**
   * TODO 07 Nonblocking backpressure
   *
   * Look at the code, you'll see a (fast) publisher/flux and a (slow) subscriber/processor.
   * Run the test and notice that both the producer & subscriber run synchronously: emitting 1 element & processing it before the next element is emitted.
   * In this case the slowest component (= the subscriber) in the chain dictates the speed of the emitter/publisher.
   *
   * This is not what we want. We don't want the subscriber dictating the publishers speed and on the other hand the publisher should
   * not dictate the speed of how fast the subscriber can process the data. Both the Publisher/subscriber should be able to transmit/process data
   * at their own pace without affecting each other. Let's do this!
   *
   * Uncomment the publishOn method and re-run the test. Now you will see that the publisher and subscriber will be working on their own pace.
   * The publisher (emitting data) will finish earlier than the subscriber. So they work async now because 'publishOn()' influences the threading
   * context where the rest of the operators in the chain below it will execute, up to a new occurrence of publishOn.
   *
   * You might have noticed that despite the faster speed of the publisher, all elements are eventually being sent down to the subscriber/processor.
   * That is because, by default, the publisher will buffer elements in case of a slow subscriber, emitting the elements when the subscriber is ready for the next.
   * Let's change this to a drop-strategy by using the correct create method on Flux. Now run the test again and notice that some elements will not be processed
   * because they are being dropped by the publisher.
   */
  @Test
  public void backPressure() {
    Flux<Integer> numbers = Flux.create(emitter -> emit(emitter));

    numbers
           //.publishOn(Schedulers.parallel(), 1)
           .map(e -> e * 1)
           .subscribe(e -> slowProcessor(e),
                      err -> System.out.println(err),
                      () -> System.out.println("DONE"));

    sleep(10000);
  }

  private void slowProcessor(Integer i) {
    System.out.println("   >> processing: "+i);
    sleep(1000);
  }

  private void emit(FluxSink<Integer> emitter) {
    int count = 0;

    while (count < 10) {
      count++;
      sleep(250);
      System.out.println("emitting : "+count);
      emitter.next(count);
    }

    emitter.complete();
  }

  /**
   *  TODO 08 Backpressure: subscriber in full control
   *
   *  A different strategy on backpressure is to have the subscriber ask for the next x elements when ready.
   *  That's what we'll do here.
   *
   * Run the test and notice that nothing is printed, so let's see why.
   *
   *  As you can see below, we created our own Subscriber. The subscriber interface has 4 main methods :
   *    onSubscribe()
   *    onNext()
   *    onError()
   *    onComplete()
   *
   *  In the onNext() method we print the element we receive from the publisher, but apparently no element is emitted. That is because
   *  the publisher assumes the worst, namely that the capacity of the subscriber is 0 (= subscriber can't handle any transmission).
   *
   *  Let's change this in the onSubscribe() method by requesting 1 element.
   *  Run the test again and see how you now get 1 element transmitted (* 1)
   *
   *  What happened to the other elements? Well you still have to request for a new element when the previous element got processed.
   *  Do this by updating the onNext() method, requesting 1 new element.
   *
   *  Run the test and now you should see all elements flowing through the pipeline.
   *
   *  Depending on what you want your initial capacity to be, you could change the onSubscribe() method to request 1 or more elements the first time.
   *  Once you have processed 1 element you increase your capacity with 1, requesting the next element (in the onNext()), so you get a constant buffer.
   *
   *  To conclude: you as a subscriber now have full control over the number of elements you receive and you can buffer them if needed
   */
  @Test
  public void backPressureFullControl() {
    Flux<Integer> numbers = Flux.range(1, 10);

    numbers.subscribe(new Subscriber<Integer>() {
      private Subscription subscription;

      @Override
      public void onSubscribe(Subscription s) {
        this.subscription = s;
      }

      @Override
      public void onNext(Integer integer) {
        System.out.println("* "+integer);
      }

      @Override
      public void onError(Throwable t) {
        System.out.println("ERROR ...");
      }

      @Override
      public void onComplete() {
        System.out.println("DONE ...");
      }
    });

    sleep(2000);
    System.out.println("**** END ****");
  }

}