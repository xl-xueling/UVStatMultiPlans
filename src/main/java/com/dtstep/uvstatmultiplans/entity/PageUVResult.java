package com.dtstep.uvstatmultiplans.entity;

import java.io.Serializable;

public class PageUVResult implements Serializable {

    public String page;

    public long windowTime;

    public long uv;


    public static PageUVResult of(String page, long windowEnd, long uv) {
        PageUVResult result = new PageUVResult();
        result.page = page;
        result.windowTime = windowEnd;
        result.uv = uv;
        return result;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public long getWindowTime() {
        return windowTime;
    }

    public void setWindowTime(long windowTime) {
        this.windowTime = windowTime;
    }

    public long getUv() {
        return uv;
    }

    public void setUv(long uv) {
        this.uv = uv;
    }
}
