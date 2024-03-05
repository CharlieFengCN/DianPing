package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private void hanleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if (!isLock) {
            //获取锁失败
            log.error("阻塞队列出现错误：不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {

            while (true) {
                //获取订单中的信息
                try {
                    //获取消息队列中的订单信息 XREADGRUP GRUOP g1 c1 COUNT 1 BLOCK 2000 STREAM stream.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //判断是否成功
                    if (list == null || list.isEmpty()) {
                        continue;
                    }
                    //解析消息中的订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //如果没成功 说明没有消息 继续循环
                    //成功 可以下单
                    hanleVoucherOrder(voucherOrder);
                    //确认消息队列中的ACK SACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());


                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    try {
                        handlePendingList();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }

        private void handlePendingList() throws InterruptedException {
            while (true) {
                //获取订单中的信息
                try {
                    //获取消息队列中的订单信息 XREADGRUP GRUOP g1 c1 COUNT 1 BLOCK 2000 STREAM stream.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //判断是否成功
                    if (list == null || list.isEmpty()) {
                        //说明pendinglist没有消息
                        break;
                    }
                    //解析消息中的订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //如果没成功 说明没有消息 继续循环
                    //成功 可以下单
                    hanleVoucherOrder(voucherOrder);
                    //确认消息队列中的ACK SACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理Pending-List订单异常", e);
                    Thread.sleep(20);
                }
            }
        }
    }

    /*private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                //获取订单中的信息
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    //创建订单即写入数据库
                    hanleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        //1、执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT, Collections.emptyList(),
                voucherId.toString(), userId.toString(),
                String.valueOf(orderId)
        );
        //2、判断结果是否为0
        int r = result.intValue();
        if (r != 0) {//2、1、不为0没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //返回订单id
        return Result.ok(orderId);

    }
    /*public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //1、执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT, Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        //2、判断结果是否为0
        int r = result.intValue();
        if (r != 0) {//2、1、不为0没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        //为0有购买资格，将下单信息保存到阻塞队列
        long orderId = redisIdWorker.nextId("order");
        //保存消息阻塞队列
        //6、创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //6.1订单id
        voucherOrder.setId(orderId);
        //用户id
        voucherOrder.setUserId(userId);
        //6.3代金券id
        voucherOrder.setVoucherId(voucherId);
        //创建阻塞队列
        orderTasks.add(voucherOrder);
        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //返回订单id
        return Result.ok(orderId);

    }*/

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        //1、根据id查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2、优惠券存在 判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始");
        }
        //3、是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束");
        }
        //4、开始 判断库存是否充足
        if (voucher.getStock() < 1) {
            return Result.fail("秒杀券已抢光");
        }
        Long userId = UserHolder.getUser().getId();
        //创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if (!isLock) {
            //获取锁失败
            return Result.fail("不允许重复下单");
        }
        try {
            //获取代理对象（事物）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }
    }*/

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //一人一单
        // 6.2用户id
        Long userId = voucherOrder.getUserId();

        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("创建订单时显示该用户已经购买过");
            return;
        }
        //查询订单
        //5、库存充足 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();
        if (!success) {
            log.error("创建订单时显示库存不足");
            return;
        }
        save(voucherOrder);
    }
}
