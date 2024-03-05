package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;

/**
 * <p>
 *  服务类
 * </p>
 */
public interface IShopService extends IService<Shop> {


    Object queryById(Long id);

    Result update(Shop shop);

    Result queryShopById(Integer typeId, Integer current, Double x, Double y);
}
