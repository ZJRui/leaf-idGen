package com.sachin.idgen.leaf.core.segment;

import com.sachin.idgen.leaf.core.IDGen;
import com.sachin.idgen.leaf.core.common.Result;
import com.sachin.idgen.leaf.core.common.Status;
import com.sachin.idgen.leaf.core.segment.dao.IDAllocDao;
import com.sachin.idgen.leaf.core.segment.model.LeafAlloc;
import com.sachin.idgen.leaf.core.segment.model.Segment;
import com.sachin.idgen.leaf.core.segment.model.SegmentBuffer;
import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author Sachin
 * @Date 2022/3/27
 **/
public class SegmentIDGenImpl implements IDGen {
    public static final Logger logger = LoggerFactory.getLogger(SegmentIDGenImpl.class);


    /**
     * IDCache未初始化成功时的异常码
     */
    public static final long EXCEPTION_ID_IDCACHE_INIT_FALSE = -1;

    /**
     * 业务tag不存在时异常吗
     */
    public static final long EXCEPTION_ID_KEY_NOT_EXISTS=-2;

    /**
     * SegmentBuffer中的两个Segment均未从db中装载时的异常码
     */
    public static final long EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL = -3;
    private ExecutorService service = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new UpdateThreadFactory());

    /**
     * 一个Segment维持时间为15分钟
     */
    private static final long SEGMENT_DURATION = 15 * 60 * 1000L;
    /**
     *
     * test
     * 最大步长不超过100,0000
     */
    private static final int MAX_STEP = 1000000;
    private IDAllocDao idAllocDao;
    private volatile boolean initOk = false;
    private Map<String, SegmentBuffer> cache = new ConcurrentHashMap<>();

    public static class UpdateThreadFactory implements ThreadFactory {
        private static int threadInitNumber = 0;

        private static synchronized int nextThreadNum() {
            return threadInitNumber++;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "Thread-segment-update-" + nextThreadNum());
        }
    }



    @Override
    public Result get(String key) {
        /**
         * 如果get的线程 发现SegmentIDGenImpl没有初始化完成则 返回错误
         */
        if (!initOk) {
            return new Result(EXCEPTION_ID_IDCACHE_INIT_FALSE, Status.EXCEPTION);
        }
        if (cache.containsKey(key)) {
            /**
             * 这个get的Buffer可能为空
             *其实我觉得还是有问题，只不过是发生几率很小。
             * updateCahceFromDb逻辑中对cache的操作除了有put，还有remove，remove是问题所在。
             * 如果remove发生在cache.containsKey(key) 和 cache.get(key)之间，空指针就会出现。
             *
             *if contains then get模式我觉得做一些必要的判断是好的，毕竟两步不是原子操作。
             *
             * 【如果remove发生在cache.containsKey(key) 和 cache.get(key)之间，空指针就会出现。】从线程安全来说，确实会有这个问题，虽然发生的概率很小很小。
             *
             */
            SegmentBuffer buffer = cache.get(key);
            /**
             * 判断Buffer是否初始化完成
             */
            if (!buffer.isInitOk()) {
                //buffer 没有初始化,加同步锁，保证只有一个线程对SegmentBuffer进行初始化
                synchronized (buffer) {
                    if (!buffer.isInitOk()) {
                        try {
                            Segment current = buffer.getCurrent();
                            updateSegmentFromDB(key, current);
                            logger.info(" init buffer ,update leafkey {} {} from db", key, buffer.getCurrent());
                            /**
                             * 因为 initOk这个属性存在多线程访问的问题，因此需要 使用volatile修饰
                             */
                            buffer.setInitOk(true);
                        } catch (Exception e) {
                            logger.warn("init buffer {} exception", buffer.getCurrent(), e);
                        }
                    }
                }
            }
            // buffer 已经初始化完成
            return getIdFromSegmentBuffer(cache.get(key));
        }
        /**
         * 业务tag不存在
         */
        return new Result(EXCEPTION_ID_KEY_NOT_EXISTS, Status.EXCEPTION);
    }

    /**
     * 从数据库中读取指定 业务的 Segment
     * @param key
     * @param curSegment
     */
    private void updateSegmentFromDB(String key, Segment curSegment) {

        StopWatch stopWatch = new Slf4JStopWatch();
        SegmentBuffer buffer = curSegment.getBuffer();
        LeafAlloc leafAlloc=null;

        /**
         * Buffer尚未初始化
         */
        if (!buffer.isInitOk()) {
            /**
             * 首先将 指定的业务tag的 maxId 自增step， 比如maxId=90,step=10，则update之后maxId=100
             * 然后读取指定业务tag的 记录数据
             */
            leafAlloc=idAllocDao.updateMaxIdAndGetLeafAlloc(key);
            /**
             * 问题，为什么从数据库读取 放置到Buffer之后 为什么没有更新Buffer的 updateTimestamp字段
             */
            buffer.setStep(leafAlloc.getStep());
            buffer.setMinStep(leafAlloc.getStep());
        } else if (buffer.getUpdateTimestamp() == 0) {
            /**
             * 什么情况下会出现 initOk为true，但是 updateTimeStamp==0呢？
             *考虑这种情况： 线程A 取号， 首先是SegmentBuffer没有 init，因此get的时候 会执行updateSegmentFromDB 方法，从而执行了上面的
             * 第一个if。这个第一个if会初始化当前的Segment， updateSegmentFromDB执行完了之后 会设置SegmentBuffer的initOk为true。
             * 然后线程A 不断通过 getIdFromSegmentBuffer 方法从Segment中取出id。 在getIdFromSegmentBuffer方法中会判断 当前Segment中剩余的可用id
             * 达到阈值的时候 会启动一个线程 通过updateSegmentFromDb方法准备 下一个Segment， 也就是说这个时候 就满足 SegmentBuffer的initOk为true，但是updateTimestamp=0
             * 然后这个时候 也会从数据库中读取数据到Buffer中，然后Buffer初始化另外一个Segment。
             *
             * ==================
             * 考虑一种情况： （1）线程A取号，当达到阈值的时候启动线程B准备下一个Segment， 如果在线程B准备完成之前线程A已经消耗完了 当前Segment中的所有id 那么这个时候线程A如何处理？
             *
             *
             */
            leafAlloc = idAllocDao.updateMaxIdAndGetLeafAlloc(key);
            /**
             * 为什么上面 不设置 updateTimeStamp？
             */
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(leafAlloc.getStep());
            buffer.setMinStep(leafAlloc.getStep());

        }else{

            /**
             * 问题 SegmentBuffer的 initOk属性 只会在get方法中 被调用，而且 从代码上看 get方法中 会获取到Buffer锁对象 然后判断
             * isInitOk为false的情况下才会执行 updateSegmentFromDB， 也就意味着 理论上updateSegmentFromDB方法内 SegmentBuffer对象的
             * isInitOk为false（错误，只有在get内调用当前方法的时候才为false）。 然后当UpdateSegmentFromDB方法执行完之后才会将 SegmentBuffer的initOk属性设置为true。
             * 所以什么情况下 会走到这里？
             * 当前方法 还会被 getIdFromSegmentBuffer 方法调用。这个时候可能走到这里。
             *
             * 走到这里 就说明 SegmentBuffer中的两个Segment 都已经初始化过一次了
             */
            long duration = System.currentTimeMillis() - buffer.getUpdateTimestamp();
             int nextStep = buffer.getStep();
            if (duration < SEGMENT_DURATION) {
                if (nextStep * 2 > MAX_STEP) {

                }else{
                    nextStep = nextStep * 2;
                }
            } else if (duration < SEGMENT_DURATION * 2) {

            }else{
                nextStep = nextStep / 2 >= buffer.getMinStep() ? nextStep / 2 : nextStep;
            }
            logger.info("leafKey[{}], step[{}], duration[{}mins], nextStep[{}]", key, buffer.getStep(), String.format("%.2f",((double)duration / (1000 * 60))), nextStep);
            LeafAlloc temp = new LeafAlloc();
            temp.setKey(key);
            temp.setStep(nextStep);
             leafAlloc = idAllocDao.updateMaxIdByCustomStepAndGetLeafAlloc(temp);
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(nextStep);
            buffer.setMinStep(leafAlloc.getStep());
        }

        /**
         * 设置segment中 可用的开始值 ，比如maxid=100, step=10, 那么表示之前的maxId=90，
         * 问题： 在上面的第一个if buffer.isInitOk为false的情况下 会读取数据库设置buffer，为什么下面没有 设置Buffer的updateTimeStamp字段？
         *
         */
        long value = leafAlloc.getMaxId() - buffer.getStep();
        curSegment.getValue().set(value);
        curSegment.setMax(leafAlloc.getMaxId());
        curSegment.setStep(buffer.getStep());
        stopWatch.stop("updateSegmentFromDB", key + " " + curSegment);
    }

    private Result getIdFromSegmentBuffer(SegmentBuffer segmentBuffer) {

        while (true) {

            /**
             * 当前线程从SegmentBuffer中的segment中取号的时候 需要 阻止其他线程 修改SegmentBuffer当前使用的segment的下标号，放置切换Segment，
             * 因此需要加读锁
             */
            segmentBuffer.rLock().lock();
            try{
                final Segment curSegment = segmentBuffer.getCurrent();

                /**
                 * 校验当前被使用的Segment 内剩余取号
                 *
                 * threadRunning主要用来保证只有一个线程 执行对 nextSegment的准备工作。
                 */
                if (!segmentBuffer.isNextReady() && (curSegment.getIdle() < 0.9 * curSegment.getStep()) && segmentBuffer.getThreadRunning().compareAndSet(false, true)) {
                    service.execute(new Runnable() {
                        @Override
                        public void run() {
                            final Segment nextSegment = segmentBuffer.getSegments()[segmentBuffer.nextPos()];
                            boolean updateOk=false;
                            try {
                                updateSegmentFromDB(segmentBuffer.getKey(), nextSegment);
                                updateOk = true;
                                logger.info("update segment {} from db{}", segmentBuffer.getKey(), nextSegment);
                            } catch (Exception e) {
                                logger.warn(segmentBuffer.getKey() + " updateSegmentFromDb exception", e);
                            }finally {
                                if (updateOk) {
                                    /**
                                     * 修改SegmentBuffer的属性 需要加锁. 注意这里只更新 下一个segment是否准备就绪，并没有切换正在使用的Segment
                                     * 因为线程A取号，线程A如果发现当前Segment中没有可用 id了则线程A负责切换Segment； 线程B只负责 准备下一个Segment中的数据，
                                     * 不负责切换Segment。
                                     * 需要注意的是 线程A  如果当前Segment没有可用id，且发现下一个Segment 已经准备好了，那么线程A切换Segment，这个操作是需要对SegmentBuffer加写锁，
                                     * 那么问题就是： 先加锁然后判断 下一个segment是否可用呢  还是先判断下一个Segment是否可用然后对SegmentBuffer加锁呢？
                                     */
                                    segmentBuffer.wLock().lock();
                                    segmentBuffer.setNextReady(true);
                                    segmentBuffer.getThreadRunning().set(false);
                                    segmentBuffer.wLock().unlock();
                                }else{
                                    segmentBuffer.getThreadRunning().set(false);
                                }
                            }
                        }
                    });

                }
                //------------------------------------获取当前可用id
                final long value = curSegment.getValue().getAndIncrement();
                if (value < curSegment.getMax()) {
                    /**
                     * 取号成功返回之前需要校验 剩余可用id是否达到阈值。
                     *
                     * 实际上不单单是取号成功返回之前需要校验，应该在每次取号之前都需要校验，不管当前取号是否成功。
                     * 其次校验的时候需要针对当前SegmentBuffer 使用的Segment进行校验， 也就是说校验的过程中 需要保证 其他线程不能修改SegmentBuffer中使用的Segment下标。
                     * 因此这个校验过程需要加锁，可以放在当前的 SegmentBuffer的读锁内完成。 这就为什么 上面的校验代码在前面的原因。
                     *
                     */
                    return new Result(value, Status.SUCCESS);
                }

            }finally {
                segmentBuffer.rLock().unlock();
            }

            /**
             * 线程运行到这里 则意味着  当前segment 中已经没有可用的value了。 可能 同一时间有多个线程 阻塞在取号这里
             */
            waitAndSleep(segmentBuffer);

            /**
             * 线程 从waitAndSleep方法中返回有三种情况
             * （1）while条件不满足，也就是threadRunning=false，意味着 线程B准备nextSegment工作结束
             *     A:如果线程B准备nextSegment成功， 则SegmentBuffer的nextReady为true，
             *     B：如果线程B准备nextSegment失败， 则SegmentBuffer的nextReady为false
             * （2） while条件满足，但是当前线程A 进行了sleep之后 break，这个时候线程B正在执行 nextSegment设置工作。
             *
             * 因此我们需要判断SegmentBuffer的nextSegment是否准备就绪。
             *
             * 但是在判断之前 我们考虑这种情况， 线程 A,C,D 可能会同时判断，而且理论上 如果 SegmentBuffer的nextReady为true的话 需要同步
             * 切换Segment，切换之后再设置 nextReady为false，因此这个操作需要写加锁
             */

            if (!segmentBuffer.isNextReady()) {
                /**
                 * 1，因为线程B的异常退出导致 nextSegment没有准备好，
                 * 2.又或者线程B已经将NextSegment准备好了，但是因为线程C发现的比较早，执行了Segment的切换工作。
                 * 这个时候只需要 执行正常的取号流程就可以了
                 */
                continue;
            }
            /**
             * SegmentBuffer已经准备好下一个Segment了，且尚未有其他线程 切换Segment. 当前线程尝试切换Segment，首先需要获取写锁
             */
            segmentBuffer.wLock().lock();
            try{

                /**
                 * 再次判断 SegmentBuffer是否准备就绪，防止其他线程已经 切换了Segment
                 *
                 * 问题：可能会有这种情况， 有100个线程 阻塞在取号的地方， 线程B 正确设置了nextSegment， 然后线程C执行了切换Segment的工作，
                 * 切换之后线程C和其他线程 从新的segment中取号（但是线程A还未从新的segment中取号，线程A尚未唤醒）。然后线程D发现新的segment剩余id不多了，然后又让线程E 准备next_N_segment，线程E准备好了，但是这个时候nextSegment中
                 * 尚有 剩余的segment。 这个时候线程A唤醒了， 线程A本来是想从nextSegment中取号。 但是 线程A发现SegmentBuffer的nextReady属性为true，就擅自切换到了next_N_Segment。
                 * 这个时候就会导致nextSegment中部分号码没有使用。
                 *
                 * 也就是说 切换segment的条件是 1，nextSegment准备就绪 2，当前Segment中没有可用的id。
                 *
                 */

                final Segment newCurSegment = segmentBuffer.getCurrent();

                final long segNewValue = newCurSegment.getValue().getAndIncrement();
                if (segNewValue < newCurSegment.getMax()) {
                    /**
                     * 意味着当前尚 没有必要切换，或者已经被其他线程切换了Segment
                     */
                    return new Result(segNewValue, Status.SUCCESS);
                }
                if (segmentBuffer.isNextReady()) {
                    segmentBuffer.switchPos();
                    segmentBuffer.setNextReady(false);
                }

            }finally{
                segmentBuffer.wLock().unlock();

            }

        }//while

    }


    /**
     * 源码中 getIdFromSegmentBuffer的实现
     * @param buffer
     * @return
     */
    public Result getIdFromSegmentBuffer_originalSource(final SegmentBuffer buffer) {
        while (true) {
            buffer.rLock().lock();
            try {
                final Segment segment = buffer.getCurrent();
                if (!buffer.isNextReady() && (segment.getIdle() < 0.9 * segment.getStep()) && buffer.getThreadRunning().compareAndSet(false, true)) {
                    service.execute(new Runnable() {
                        @Override
                        public void run() {
                            Segment next = buffer.getSegments()[buffer.nextPos()];
                            boolean updateOk = false;
                            try {
                                updateSegmentFromDB(buffer.getKey(), next);
                                updateOk = true;
                                logger.info("update segment {} from db {}", buffer.getKey(), next);
                            } catch (Exception e) {
                                logger.warn(buffer.getKey() + " updateSegmentFromDb exception", e);
                            } finally {
                                if (updateOk) {
                                    buffer.wLock().lock();
                                    buffer.setNextReady(true);
                                    buffer.getThreadRunning().set(false);
                                    buffer.wLock().unlock();
                                } else {
                                    buffer.getThreadRunning().set(false);
                                }
                            }
                        }
                    });
                }
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }
            } finally {
                buffer.rLock().unlock();
            }
            waitAndSleep(buffer);
            /**
             *
             *
             * 在源码的实现中， waitAndSleep方法返回之后 直接获取写锁。
             * 在上面的 解释中 我们已经说明了 切换Segment的条件是 (1)nextSegment准备就绪 （2）当前Segment没有可用的id 因此下面的
             * codeA毫无争议。
             * 在下面 的if  中如果 nextReady为false ，则意味着 nextSegment没有准备就绪，这个时候 会返回取号失败的结果。 在我上面的代码
             * 中 并没有 返回取号失败的情况。
             *
             *
             * 我们考虑将  nextReady为false的这种情况 放在  写锁里面 会有什么危害，
             * 第一种情况： 假设线程A 和其他100个线程都阻塞在取号waitAndSleep。
             * 然后线程B 准备nextSegment失败了， 这个时候 这些线程从wait方法中返回   每一个线程都会尝试获取锁。 但是获取锁之后都会
             * 发现当前Segment不可用，且nextSegment没有准备好，然后这些 线程 就会 返回 取号失败的结果。
             * 如果我们在wait方法返回之后先判断 nextReady 没有准备好的话 就执行正常的获取流程，那么就不需要让这些线程依次获取锁。
             * 这些线程只需要 判断阈值，启动新的线程准备nextSegment，然后从当前segment中 取值 ，发现没有可用id 当前线程返回取号失败就可以了。
             *
             * 第二种情况：
             *  线程B准备 nextSegment成功，然后线程C 第一个发现 然后切换segment。 然后线程A和其他的被阻塞的线程 却需要 一次获取写锁 来取号。这显然不合理。
             *
             *
             *
             *
             *
             */
            buffer.wLock().lock();
            try {
                //---------------------- codeA------
                final Segment segment = buffer.getCurrent();
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }
                if (buffer.isNextReady()) {
                    buffer.switchPos();
                    buffer.setNextReady(false);
                    //----------------------end codeA-----
                } else {
                    logger.error("Both two segments in {} are not ready!", buffer);
                    return new Result(EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL, Status.EXCEPTION);
                }
            } finally {
                buffer.wLock().unlock();
            }
        }
    }


    private void waitAndSleep(SegmentBuffer segmentBuffer) {
        int roll = 0;
        while (segmentBuffer.getThreadRunning().get()) {
            roll += 1;
            /**
             * 线程A五号不成功的时候 就会阻塞 等待下一个 Segment准备就绪， 问题 为什么线程A 不使用Object.wait（线程C、d可能也会同时阻塞在取号的地方），然后线程B准备下一个Segment就绪
             * 之后直接调用Object.notifyAll方法同时唤醒所有阻塞等待的线程呢？  而是使用了下面的方法进行阻塞？
             *
             * 首先潘安 如果roll大于10000才会进行sleep，那么以为者当roll<10000的时候并不阻塞，而是忙循环，也就是自旋，从而避免了切换cpu。
             *
             * 第二个问题就是 下面的sleep 方法中对 InterruptedException异常处理是否恰当。
             *
             * 第三个问题： while中的条件是 判断 threadRunning 而不是判断 SegmentBuffer的nextReady属性 这是为什么？ 考虑线程B 如果准备nextSegment失败了，
             * 也就是线程B 没有将 SegmentBuffer的nextReady属性设置为true，然后就退出了。 那么这个时候就没有任何线程在准备 nextSegment了。此时线程A要 提交另一个 准备nextSegment的任务
             * 因此线程A 是否运行 取决于 threadRunning，也就是取决于是否有线程正在准备nextSegment， 如果没有（threadRunning=false）那么可能有两种情况（1）准备线程已经准备好了nextSegment
             * （2）准备线程没有 正确设置nextSegment 导致退出了 这个时候需要线程A 及时开启另一个准备线程。
             *
             *
             * 第四个问题：即便 threadRunning为true，也就是满足while条件，但是如果当前线程sleep了一段时间 然后又获的cpu，这个时候会break。 但是这个时候线程B 还未完成 nextSegment的设置工作。
             *
             *
             */
            if (roll > 10000) {
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                    break;

                } catch (InterruptedException e) {
                    logger.warn("Thread{} interrupted", Thread.currentThread().getName());
                    break;
                }

            }//end if
        }
    }

    @Override
    public boolean init() {
        logger.info("init ...");
        updateCacheFromDB();
        initOk = true;
        updateCacheFromDBAtEveryMinute();
        return initOk;
    }

    private void updateCacheFromDBAtEveryMinute() {
        ScheduledExecutorService service= Executors.newSingleThreadScheduledExecutor((r)->{
            Thread t = new Thread(r);
            t.setName("check-idCache-thread");
            t.setDaemon(true);
            return t;
        });
        service.scheduleWithFixedDelay(() -> {
            updateCacheFromDB();
        }, 60, 60, TimeUnit.SECONDS);
    }

    private void updateCacheFromDB() {

        logger.info("update cache from DB");
        StopWatch sw = new Slf4JStopWatch();
        try {

            List<String> allTags = idAllocDao.getAllTags();
            if (allTags == null || allTags.isEmpty()) {
                return;
            }
            /**
             * 当前缓存中已经存在的 业务tag
             */
            List<String> cacheTags = new ArrayList<>(cache.keySet());
            /**
             * 数据库中所有的业务tag
             */
            Set<String> insertTagSet = new HashSet<>(allTags);
            Set<String> removeTagsSet = new HashSet<>(cacheTags);
            /**
             * 对于存在于数据库中但是不存在与 当前缓存中的业务tag 创建Segment
             */
            for (int i = 0; i < cacheTags.size(); i++) {
                String tmp = cacheTags.get(i);
                if (insertTagSet.contains(tmp)) {
                    insertTagSet.remove(tmp);
                }
            }
            /**
             * 为业务创建SegmentBuffer
             */
            for (String tag : insertTagSet) {
                SegmentBuffer segmentBuffer = new SegmentBuffer();
                segmentBuffer.setKey(tag);
                Segment currentSegement = segmentBuffer.getCurrent();
                currentSegement.setValue(new AtomicLong(0));
                currentSegement.setMax(0);
                currentSegement.setStep(0);
                cache.put(tag, segmentBuffer);
            }

            /**
             * 只存在于cache 不存在数据库中的业务tag 是失效的
             *
             */
            for (int i = 0; i < allTags.size(); i++) {
                String tmp = allTags.get(i);
                if (removeTagsSet.contains(tmp)) {
                    removeTagsSet.remove(tmp);
                }
            }
            for (String tag : removeTagsSet) {
                cache.remove(tag);
            }
        } catch (Exception e) {
            logger.warn("update cache from db exception ,", e);
        } finally {
            sw.stop("updateCachefromDb");
        }
    }


    public List<LeafAlloc> getAllLeafAllocs(){
        return idAllocDao.getAllLeafAllocs();
    }
    public Map<String,SegmentBuffer> getCache(){
        return cache;
    }

    public IDAllocDao getIdAllocDao() {
        return idAllocDao;
    }

}
