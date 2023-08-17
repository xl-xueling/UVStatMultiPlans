package com.dtstep.uvstatmultiplans.plans.plan5;

import com.dtstep.uvstatmultiplans.entity.PageUVResult;
import com.dtstep.uvstatmultiplans.entity.UserBehavior;
import com.dtstep.uvstatmultiplans.redis.RedisHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HyperLogProcessWindowFunction extends ProcessWindowFunction<UserBehavior, PageUVResult, String, TimeWindow> {

    private static final StateTtlConfig stateTtlConfig = StateTtlConfig
            .newBuilder(Time.hours(3))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

    private transient MapState<String,Boolean> userMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<String, Boolean> userMapDescriptor = new MapStateDescriptor( "userIsExistMap", String.class, Boolean.class);
        userMapDescriptor.enableTimeToLive(stateTtlConfig);
        userMapState = getRuntimeContext().getMapState(userMapDescriptor);
    }

    @Override
    public void process(String page, Context context, Iterable<UserBehavior> iterable, Collector<PageUVResult> collector) throws Exception {
        List<String> userIdList = new ArrayList<>();
        for (UserBehavior userBehavior : iterable) {
            String userId = userBehavior.getUserId();
            if (!userMapState.contains(userId)) {
                userMapState.put(userId, true);
                userIdList.add(userId);
            }
        }
        String hyperKey = "hyper_key_"+page+"_" + context.window().getEnd();
        if(CollectionUtils.isNotEmpty(userIdList)){
            RedisHandler.getInstance().pfadd(hyperKey,userIdList,(int) TimeUnit.DAYS.toSeconds(1));
        }
        long value = RedisHandler.getInstance().pfcount(hyperKey);
        collector.collect(PageUVResult.of(page,context.window().getEnd(),value));
    }
}
