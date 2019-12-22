package com.share;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


 //Created by songjian on 7/4/2018.


public class RedisLimiter {

    private double storedPermits;
    private double maxPermits;
    private double stableIntervalMicros;
    private long nextFreeTicketMicros;
    private double maxBurstSeconds;

    private String permitsKey;
    private String timerKey;
    private String lockKey;

    public static JedisCluster jedisCluster;


    public RedisLimiter(Double maxBurstSeconds,Double permitsPerSecond,String baseKey) throws Exception {
        initKye(baseKey);
        //加上分布式锁
        String identifier = lockWithTimeout(lockKey,30,15);
        this.maxBurstSeconds = maxBurstSeconds;
        setRate(permitsPerSecond);
        synToRedis();
        //释放锁
        releaseLock(lockKey,identifier);
    }




    //获取redis的微秒时间
    private static long readReidsMicro() {
        List<String> list = (List<String>) RedisTemplate.getInstance().eval("return redis.call('time')");
        String str1 = list.get(0);
        String str2 = StringUtils.leftPad(list.get(1),6,"0");
        return Long.parseLong(str1+str2);
    }

    private void setRate(double permitsPerSecond){
        this.resync(this.readReidsMicro());
        double stableIntervalMicros = (double) TimeUnit.SECONDS.toMicros(1L) / permitsPerSecond;
        this.stableIntervalMicros = stableIntervalMicros;
        this.doSetRate(permitsPerSecond, stableIntervalMicros);
    }

    private void doSetRate(double permitsPerSecond, double stableIntervalMicros) {
        double oldMaxPermits = this.maxPermits;
        this.maxPermits = this.maxBurstSeconds * permitsPerSecond;
        this.storedPermits = oldMaxPermits == 0.0D?0.0D:this.storedPermits * this.maxPermits / oldMaxPermits;
    }

    private void resync(long redisMicros){
        initValues(maxBurstSeconds);
        if(redisMicros > this.nextFreeTicketMicros) {
            this.storedPermits = Math.min(this.maxPermits, this.storedPermits + (double)(redisMicros - this.nextFreeTicketMicros) / this.stableIntervalMicros);
            this.nextFreeTicketMicros = redisMicros;
        }
    }
    private void synToRedis(){
       RedisTemplate.getInstance().set(permitsKey,String.valueOf(storedPermits));
       RedisTemplate.getInstance().set(timerKey,String.valueOf(nextFreeTicketMicros));
    }


    // 获取令牌

    public double acquire() {
        return this.acquire(1);
    }
    public double acquire(int permits) {
        long microsToWait;
        //加上分布式锁
        String identifier = lockWithTimeout(lockKey,30,15);
        microsToWait = this.reserveNextTicket((double)permits, this.readReidsMicro());
        synToRedis();
        //释放锁
        releaseLock(lockKey,identifier);
        this.sleepMicrosUninterruptibly(microsToWait);
        return 1.0D * (double)microsToWait / (double)TimeUnit.SECONDS.toMicros(1L);
    }
    private long reserveNextTicket(double requiredPermits, long redisMicros) {
        this.resync(redisMicros);
        long microsToNextFreeTicket = this.nextFreeTicketMicros - redisMicros;
        double storedPermitsToSpend = Math.min(requiredPermits, this.storedPermits);
        double freshPermits = requiredPermits - storedPermitsToSpend;
        long waitMicros =  (long)(freshPermits * this.stableIntervalMicros);
        this.nextFreeTicketMicros += waitMicros;
        this.storedPermits -= storedPermitsToSpend;
        return microsToNextFreeTicket;
    }

    public void sleepMicrosUninterruptibly(long micros) {
        if(micros > 0L) {
            Uninterruptibles.sleepUninterruptibly(micros, TimeUnit.MICROSECONDS);
        }

    }

    /*
     * 初始化值。
     * @param maxBurstSeconds
*/

    private void initValues(Double maxBurstSeconds){
        this.maxBurstSeconds =maxBurstSeconds;
        Boolean keyExist = RedisTemplate.getInstance().exists(permitsKey);
        if(!keyExist){
            //如果key不存在初始化相应数据
            this.nextFreeTicketMicros = 0L;
        }else{
            //如果key存在加载相应数据
            String permits =RedisTemplate.getInstance().get(permitsKey);
            this.storedPermits = Double.parseDouble(permits);
            String timer =RedisTemplate.getInstance().get(timerKey);
            this.nextFreeTicketMicros = Long.parseLong(timer);
        }
    }

    public String lockWithTimeout(String lockKey,
                                  int acquireTimeout, int lockExpire) {
        String identifier = UUID.randomUUID().toString();
        String result = null;
        try {
            // 超时时间，上锁后超过此时间则自动释放锁
            // 获取锁的超时时间，超过这个时间则放弃获取锁
            long end = System.currentTimeMillis() + acquireTimeout*1000;
            while (System.currentTimeMillis() < end) {
                if (RedisTemplate.getInstance().setnxex(lockKey, identifier,lockExpire)) {
                    // 返回value值，用于释放锁时间确认
                    result = identifier;
                    return result;
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public boolean releaseLock(String lockKey, String identifier) {
        boolean result = false;
        try {
            while (true) {
                // 通过前面返回的value值判断是不是该锁，若是该锁，则删除，释放锁
                if (identifier.equals(RedisTemplate.getInstance().get(lockKey))) {
                   RedisTemplate.getInstance().del(lockKey);
                    result = true;
                    return result;
                }
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

/*
     * 初始化key
     * @param baseKey

*/
    private void  initKye(String baseKey){
        permitsKey = baseKey + "PERMITS";
        timerKey=baseKey+"TIMER";
        lockKey =baseKey  + "LIMIT_LOCK";
    }

    public static RedisLimiter create(Double permitsPerSecond,String baseKey) throws Exception {
        return new RedisLimiter(1.0D,permitsPerSecond,baseKey);
    }





}
