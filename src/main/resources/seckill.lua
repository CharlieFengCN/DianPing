
local voucherId=ARGV[1]
local userId=ARGV[2]
local orderId=ARGV[3]

local stockKey='seckill:stock:'..voucherId
local orderkey='seckill:order:'..voucherId

--取出stock值是否充足
if(tonumber(redis.call('get',stockKey))<=0) then
    return 1
    end

--SISMENBER判断用户是否存在orderKey中
if(redis.call('sismember',orderkey,userId)==1)then
    return 2
end

--下单扣库存
redis.call('incrby',stockKey,-1)
redis.call('sadd',orderkey,userId)
--发送消息到stream队列 XADD
redis.call('xadd','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)

return 0