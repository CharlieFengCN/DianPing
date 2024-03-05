package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_FOLLOW_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IBlogService blogService;
    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result queryBlogById(Long id) {
        //查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在");
        }
        //查询有关的用户
        queryBlogUser(blog);
        //查询是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        //获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return;
        }
        Long userId = UserHolder.getUser().getId();
        //判断当前用户是否点赞
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());

        blog.setIsLike(score != null);
    }

    @Override
    public Result likeBlog(Long id) {
        //获取登录用户
        Long userId = UserHolder.getUser().getId();
        //判断当前用户是否点赞
        String key = BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        //未点赞 可以点赞
        if (score == null) {
            //点赞数据库点赞数+1
            boolean isSucces = update().setSql("liked=liked+1").eq("id", id).update();
            ///保存用户到redis set集合
            if (isSucces) {
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            //已点赞 不可以点赞
            boolean isSucces = update().setSql("liked=liked-1").eq("id", id).update();
            //取消点赞数据库点赞数-1
            ///移除用户到redis set集合
            if (isSucces) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }


        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id;
        //查询top5点赞数 key 0 4
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        //解析出用户id
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);
        //查询用户
        /*List<UserDTO> userDTOs = userService.listByIds(ids).stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList())*/
        List<UserDTO> userDTOs = userService.query().in("id", ids)
                .last(" order by field(id," + idStr + ")").list().stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        //返回

        return Result.ok(userDTOs);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = blogService.save(blog);
        //推模式
        if (!isSuccess) {
            return Result.fail("新增笔记失败");
        }
        //bolg发送给粉丝
        List<Follow> followUser = followService.query().eq("follow_user_id", user.getId()).list();
        //找到作者所有粉丝
        for (Follow follow : followUser) {
            Long userId = follow.getUserId();
            String key = FEED_FOLLOW_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        //推送笔记id给所有粉丝
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogofFollow(Long max, Integer offset) {
        //获取当前用户
        Long userId = UserHolder.getUser().getId();
        //查询收件箱
        String key = FEED_FOLLOW_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(
                key, 0, max, offset, 2);
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        //解析收件箱：blogID minTime（时间戳） offset
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            ids.add(Long.valueOf(tuple.getValue()));

            long time = tuple.getScore().longValue();
            if (time == minTime) {
                os++;
            } else {
                minTime = time;
                os = 1;
            }
        }
        //根据blogid查询blog
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("order by field(id," + idStr + ")").list();
        for (Blog blog : blogs) {
            queryBlogUser(blog);
            isBlogLiked(blog);
        }
        if(os==offset){
            os+=offset;
        }
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setOffset(os);
        r.setMinTime(minTime);
        //封装并返回
        return Result.ok(r);
    }
}
