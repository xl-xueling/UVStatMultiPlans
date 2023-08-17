package com.dtstep.uvstatmultiplans.entity;

public class UserBehavior {

    private String userId;

    private String page;

    private long behaviorTime;

    public UserBehavior(){}

    public UserBehavior(String userId,String page,long behaviorTime){
        this.userId = userId;
        this.page = page;
        this.behaviorTime = behaviorTime;
    }

    public UserBehavior(String userId,long behaviorTime){
        this.userId = userId;
        this.behaviorTime = behaviorTime;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getBehaviorTime() {
        return behaviorTime;
    }

    public void setBehaviorTime(long behaviorTime) {
        this.behaviorTime = behaviorTime;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }
}
