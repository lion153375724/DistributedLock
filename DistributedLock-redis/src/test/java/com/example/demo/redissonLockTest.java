package com.example.demo;

import org.junit.Test;

import com.example.demo.redissonlock.DistributedRedisLock;
import com.example.demo.redissonlock.RedissonManager;

public class redissonLockTest {
	
	@Test
	public void redisLock(){
		//RedissonManager.getRedisson("10.17.1.204", "6379");
        RedissonManager.init("",""); //初始化
        for (int i = 0; i < 100; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        String key = "test123";
                        DistributedRedisLock.acquire(key);
                        Thread.sleep(5000); //获得锁之后可以进行相应的处理
                        System.err.println("======获得锁后进行相应的操作======");
                        DistributedRedisLock.release(key);
                        System.err.println("=============================");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            t.start();
        }
    }
}
