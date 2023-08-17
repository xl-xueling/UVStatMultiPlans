package com.dtstep.uvstatmultiplans.trigger;

import com.dtstep.uvstatmultiplans.util.DateUtil;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.concurrent.TimeUnit;

public class TimeIntervalTrigger<T> extends Trigger<T, TimeWindow>{

    private final long intervalMills;

    public TimeIntervalTrigger(int interval, TimeUnit timeUnit){
        intervalMills = timeUnit.toMillis(interval);
    }

    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("stateDesc", new Sum(), LongSerializer.INSTANCE);

    @Override
    public TriggerResult onElement(T t, long timestamp, TimeWindow timeWindow, Trigger.TriggerContext context) throws Exception {
        ReducingState<Long> fireTimestamp = context.getPartitionedState(stateDesc);
        timestamp = context.getCurrentProcessingTime();
        if (fireTimestamp.get() == null) {
            long start = timestamp - (timestamp % intervalMills);
            long nextFireTimestamp = start + intervalMills;
            context.registerProcessingTimeTimer(nextFireTimestamp);
            fireTimestamp.add(nextFireTimestamp);
            return TriggerResult.CONTINUE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long timestamp, TimeWindow window, Trigger.TriggerContext context) throws Exception {
        ReducingState<Long> fireTimestamp = context.getPartitionedState(stateDesc);
        if (fireTimestamp.get().equals(timestamp)) {
            fireTimestamp.clear();
            fireTimestamp.add(timestamp + intervalMills);
            context.registerProcessingTimeTimer(timestamp + intervalMills);
            return TriggerResult.FIRE;
        } else if(window.maxTimestamp() == timestamp) {
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long timestamp, TimeWindow timeWindow, Trigger.TriggerContext triggerContext) throws Exception {
        return null;
    }

    @Override
    public void clear(TimeWindow timeWindow, Trigger.TriggerContext triggerContext) throws Exception {}

    private static class Sum implements ReduceFunction<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }
}

