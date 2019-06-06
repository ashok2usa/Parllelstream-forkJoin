package cla.parstr;

import java.lang.annotation.Retention;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;
import org.testng.ITestResult;
import org.testng.annotations.*;
import static java.lang.System.out;
import static java.lang.annotation.RetentionPolicy.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Listeners(ParallelStreamFjpTestListener.class)
public class ParallelStreamFjpTest {

    @Retention(RUNTIME) @interface Reproducible {}
    @Retention(RUNTIME) @interface NotReproducible {}


    static final int TEST_TIMES = 10;


    static final long STREAM_SIZE = chooseStreamSize();
    static long chooseStreamSize() {

        return 100;

    }


    static final long EXPECTED_SUM = expectedSum(STREAM_SIZE);
    static long expectedSum(long N) {
        return Math.multiplyExact(N, N + 1) / 2; //"arithmetic sum" formula
    }


    static final ForkJoinPool commonPool = ForkJoinPool.commonPool(),
                              anotherPool = new ForkJoinPool(
                                    commonPool.getParallelism(),
                                    commonPool.getFactory(),
                                    commonPool.getUncaughtExceptionHandler(),
                                    commonPool.getAsyncMode()
                              );

    static {
        out.printf("TEST_TIMES: %d, STREAM_SIZE: %d, availableProcessors: %d, parallelism: %d%n",
            TEST_TIMES,
            STREAM_SIZE,
            Runtime.getRuntime().availableProcessors(), //2
            commonPool.getParallelism()
        );

    }


    @NotReproducible @Test(invocationCount=TEST_TIMES, enabled = true)
    public void executesInCommonPool() {
        assertTrue(executesInPool(commonPool));
    }


    @Reproducible @Test(invocationCount=TEST_TIMES, enabled = true)
    public void executesInAnotherPool() {
        assertTrue(anotherPool.invoke(new ExecutesInPool(anotherPool)));
    }

    @Reproducible @Test(invocationCount=TEST_TIMES, enabled = true)
    public void executesInCommonPool_sanityCheck() {
        assertFalse(executesInPool(anotherPool));
    }

    @Reproducible @Test(invocationCount=TEST_TIMES, enabled = true)
    public void executedInAnotherPool_sanityCheck1() {
        assertFalse(commonPool.invoke(new ExecutesInPool(anotherPool)));
    }

    @Reproducible @Test(invocationCount=TEST_TIMES, enabled = true)
    public void executesInAnotherPool_sanityCheck2() {
        assertFalse(anotherPool.invoke(new ExecutesInPool(commonPool)));
    }

    @NotReproducible @Test(invocationCount=TEST_TIMES, enabled = true)
    public void executesInAnotherPool_sanityCheck3() {
        assertTrue(commonPool.invoke(new ExecutesInPool(commonPool)));
    }

    static class ExecutesInPool extends RecursiveTask<Boolean> {
        private final ForkJoinPool pool;
        ExecutesInPool(ForkJoinPool pool) { this.pool = pool; }
        @Override protected Boolean compute() { return executesInPool(pool); }
    }

    static boolean executesInPool(ForkJoinPool pool) {

        AtomicBoolean neverInUnexpectedThread = new AtomicBoolean(true),
                      atLeastOnceInPool = new AtomicBoolean(false);
        Thread testThread = Thread.currentThread();

        long sum = LongStream.rangeClosed(1L, STREAM_SIZE).parallel().reduce(0L, (s, i) -> {
            Thread lambdaThread = Thread.currentThread();


            if(!isPoolOrTestThread(lambdaThread, testThread, pool)) neverInUnexpectedThread.lazySet(false);

            if(isPoolThread(lambdaThread, pool)) atLeastOnceInPool.lazySet(true);

            return s + i;
        });
        assertEquals(EXPECTED_SUM, sum, "sum bug");//Make sure computation is really done


        return neverInUnexpectedThread.get() && atLeastOnceInPool.get();
    }


    static volatile boolean lazySetFence;

    static boolean isPoolThread(Thread lambdaThread, ForkJoinPool pool) {
        if(!(lambdaThread instanceof ForkJoinWorkerThread)) return false;
        return ((ForkJoinWorkerThread) lambdaThread).getPool() == pool;
    }

    static boolean isPoolOrTestThread(Thread lambdaThread, Thread testThread, ForkJoinPool pool) {
        return lambdaThread == testThread || isPoolThread(lambdaThread, pool);
    }

    private Instant whenTestMethodStarted;
    private long testMethodNumber = 0;
    static final int LOG_EVERY = TEST_TIMES/100 == 0 ? 1 : TEST_TIMES/100;
    @BeforeMethod public void before() {
        this.whenTestMethodStarted = Instant.now();
    }
    @AfterMethod public void after(ITestResult result) {
        if(++testMethodNumber % LOG_EVERY != 0) return; //Avoid OOME(GC overhead limit exceeded) @surefire.StreamPumper
        out.printf(
                "%s #%d took: %s%n",
                result.getMethod().getMethodName(),
                testMethodNumber,
                Duration.between(whenTestMethodStarted, Instant.now())
        );
    }

    private static void sleep(int i, TimeUnit u) {
        try {
            u.sleep(i);
        } catch (InterruptedException e) {
            return;
        }
    }
}
