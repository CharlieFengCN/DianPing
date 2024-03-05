package com.hmdp;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootTest
public class RedissonTest {
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private RedissonClient redissonClient1;
    @Resource
    private RedissonClient redissonClient2;

    private RLock lock;

    @BeforeEach
    void setup() {
        RLock lock1 = redissonClient.getLock("order");
        RLock lock2 = redissonClient1.getLock("order");
        RLock lock3 = redissonClient2.getLock("order");

        //创建连锁
        lock = redissonClient.getMultiLock(lock1, lock2, lock3);
    }

    @Test
    void method1() {
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("获取锁失败，1");
            return;
        }
        try {
            log.info("获取锁成功，1");
            method2();
        } finally {
            log.info("释放锁，1");
            lock.unlock();
        }
    }

    void method2() {
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("获取锁失败, 2");
            return;
        }
        try {
            log.info("获取锁成功，2");
        } finally {
            log.info("释放锁，2");
            lock.unlock();
        }


    }
}