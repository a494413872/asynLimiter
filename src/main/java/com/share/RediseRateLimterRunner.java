package com.share;


/**
 * Created by songjian on 7/3/2018.
 */

public class RediseRateLimterRunner {

    public static void main(String[] args) throws Exception{
        final RedisLimiter rt = RedisLimiter.create(1d,"AAAAA");

        Thread thread1 = new Thread(new Runnable()  {

            public void run() {
                System.out.println(Thread.currentThread().getName() + " ---- start");
                int isSleep = 0;
                while (true) {
                    double acquire = rt.acquire(2);
                    System.out.println(Thread.currentThread().getName() + " acquire:" + acquire );
//                    isSleep ++;
//                    if (isSleep == 9) {
//                        try {
//                            Thread.sleep(2000);
//                            System.out.println(Thread.currentThread().getName() + " sleep 2000ms time:" + System.currentTimeMillis());
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                    }
                }
            }

        });

        thread1.start();

        Thread thread2 = new Thread(new Runnable()  {
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName() + " ---- start");
                int isSleep = 0;
                while (true) {
                    double acquire = rt.acquire(2);
                    System.out.println(Thread.currentThread().getName() + " acquire:" + acquire );
//                    isSleep ++;
//                    if (isSleep == 9) {
//                        try {
//                            Thread.sleep(800);
//                            System.out.println(Thread.currentThread().getName() + " sleep 2000ms time:" + System.currentTimeMillis());
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                    }
                }
            }

        });

        thread2.start();

        Thread.sleep(Integer.MAX_VALUE);
    }
}

