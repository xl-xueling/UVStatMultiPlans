package com.dtstep.uvstatmultiplans.plans.plan2;

import com.dtstep.uvstatmultiplans.entity.PageUVResult;
import com.dtstep.uvstatmultiplans.entity.UserBehavior;
import com.dtstep.uvstatmultiplans.util.DateUtil;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class BaseStateProcessWindowFunction extends ProcessWindowFunction<UserBehavior, PageUVResult, String, TimeWindow> {

    private static final StateTtlConfig stateTtlConfig = StateTtlConfig
            .newBuilder(Time.hours(3))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

    private transient ValueState<Long> uvState;

    private transient MapState<String,Boolean> userMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Long> uvStateDescriptor= new ValueStateDescriptor("value",Long.class);
        MapStateDescriptor<String, Boolean> userMapDescriptor = new MapStateDescriptor( "userIsExistMap", String.class, Boolean.class);
        uvStateDescriptor.enableTimeToLive(stateTtlConfig);
        userMapDescriptor.enableTimeToLive(stateTtlConfig);
        uvState = getRuntimeContext().getState(uvStateDescriptor);
        userMapState = getRuntimeContext().getMapState(userMapDescriptor);
    }

    @Override
    public void process(String page, Context context, Iterable<UserBehavior> iterable, Collector<PageUVResult> collector) throws Exception {
        if (uvState.value() == null) {
            uvState.update(0L);
        }
        long uv = uvState.value();
        for (UserBehavior userBehavior : iterable) {
            String userId = userBehavior.getUserId();
            if (!userMapState.contains(userId)) {
                userMapState.put(userId, true);
                uv++;
            }
        }
        uvState.update(uv);
        collector.collect(PageUVResult.of(page,context.window().getEnd(),uv));
    }
}
