package com.netflix.discovery;

import java.util.TimerTask;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.LongGauge;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A supervisor task that schedules subtasks while enforce a timeout.
 * Wrapped subtasks must be thread safe.
 *
 * @author David Qiang Liu
 */
public class TimedSupervisorTask extends TimerTask {
    private static final Logger logger = LoggerFactory.getLogger(TimedSupervisorTask.class);

    private final Counter successCounter;
    private final Counter timeoutCounter;
    private final Counter rejectedCounter;
    private final Counter throwableCounter;
    private final LongGauge threadPoolLevelGauge;

    private final String name;
    private final ScheduledExecutorService scheduler;
    private final ThreadPoolExecutor executor;
    private final long timeoutMillis;
    private final Runnable task;

    private final AtomicLong delay;
    private final long maxDelay;

    public TimedSupervisorTask(String name, ScheduledExecutorService scheduler, ThreadPoolExecutor executor,
                               int timeout, TimeUnit timeUnit, int expBackOffBound, Runnable task) {
        this.name = name;
        this.scheduler = scheduler;
        this.executor = executor;
        // 任务超时时间就等于任务调度的间隔时间
        this.timeoutMillis = timeUnit.toMillis(timeout);
        this.task = task;
        // 延迟时间默认为超时时间
        this.delay = new AtomicLong(timeoutMillis);
        // 最大延迟时间，默认在超时时间的基础上扩大10倍
        this.maxDelay = timeoutMillis * expBackOffBound;

        // Initialize the counters and register.
        // 初始化计数器并注册
        successCounter = Monitors.newCounter("success");
        timeoutCounter = Monitors.newCounter("timeouts");
        rejectedCounter = Monitors.newCounter("rejectedExecutions");
        throwableCounter = Monitors.newCounter("throwables");
        threadPoolLevelGauge = new LongGauge(MonitorConfig.builder("threadPoolUsed").build());
        Monitors.registerObject(name, this);
    }

    @Override
    /**
     * 1）首先将任务异步提交到线程池去执行，它这里并不是直接运行任务，而是异步提交到线程池中，这样可以实现超时等待，不影响主任务
     * 2）任务如果超时，比如出现网络延迟、eureka server 不可用等情况，超时了，它这个时候就会认为如果还是30秒后调度，
     * 可能 eureka server 还是不可用的状态，那么就增大延迟时间，那么第一次超时就会在300秒后再调度。
     * 如果300秒内 eureka server 可用了，然后有新的服务实例注册上去了，那这个客户端就不能及时感知到了，
     * 因此我觉得可以将 getCacheRefreshExecutorExponentialBackOffBound 对应的参数适当设置小一点（默认10倍）。
     * 3）如果任务没有超时，在调度成功后，就会重置延迟时间为默认的超时时间。最后在 finally 中进行下一次的调度。
     */
    public void run() {
        Future<?> future = null;
        try {
            // 提交任务到线程池
            future = executor.submit(task);
            threadPoolLevelGauge.set((long) executor.getActiveCount());
            // 阻塞直到任务完成或超时
            future.get(timeoutMillis, TimeUnit.MILLISECONDS);  // block until done or timeout
            // 任务完成后，重置延迟时间为超时时间，即30秒
            delay.set(timeoutMillis);
            threadPoolLevelGauge.set((long) executor.getActiveCount());
            // 成功次数+1
            successCounter.increment();
        } catch (TimeoutException e) {
            logger.warn("task supervisor timed out", e);
            // 超时次数+1
            timeoutCounter.increment();

            // 如果任务超时了，就会增大延迟时间，当前延迟时间*2，然后取一个最大值
            long currentDelay = delay.get();
            long newDelay = Math.min(maxDelay, currentDelay * 2);

            // 设置为最大的一个延迟时间
            delay.compareAndSet(currentDelay, newDelay);

        } catch (RejectedExecutionException e) {
            if (executor.isShutdown() || scheduler.isShutdown()) {
                logger.warn("task supervisor shutting down, reject the task", e);
            } else {
                logger.warn("task supervisor rejected the task", e);
            }

            rejectedCounter.increment();
        } catch (Throwable e) {
            if (executor.isShutdown() || scheduler.isShutdown()) {
                logger.warn("task supervisor shutting down, can't accept the task");
            } else {
                logger.warn("task supervisor threw an exception", e);
            }

            throwableCounter.increment();
        } finally {
            if (future != null) {
                future.cancel(true);
            }

            if (!scheduler.isShutdown()) {
                // 延迟 delay 时间后，继续调度任务
                scheduler.schedule(this, delay.get(), TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public boolean cancel() {
        Monitors.unregisterObject(name, this);
        return super.cancel();
    }
}