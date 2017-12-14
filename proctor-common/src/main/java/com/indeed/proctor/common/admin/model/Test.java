package com.indeed.proctor.common.admin.model;

import java.util.Date;
import java.util.List;

public class Test {

    private long id;

    private String testId;  // 实验ID

    private String name;   // 实验名称

    private String description; //实验描述

    private String state; //实验状态 preparing, running, paused, ended 准备 运行中 暂停 终止
    public final static String STATE_PREPARING = "preparing";
    public final static String STATE_RUNNING = "running";
    public final static String STATE_PAUSED = "paused";
    public final static String STATE_ENDED = "ended";

    private String type;  //类型 deviceId uid
    public final static String TYPE_DEVICEID = "deviceId";
    public final static String TYPE_UID = "uid";

    private String platform; //平台 ios android
    public final static String PLATFORM_IOS = "ios";
    public final static String PLATFORM_ANDROID = "android";

    private List<String> targets; //目标用户new all
    public final static String TARGET_NEW = "new";
    public final static String TARGET_ALL = "all";

    private List<String> excludes; //互斥实验ID

    private List<TestGroup> testGroups; //实验分组

    private int ratio; //放量比例 万分之x 1-1000

    private Date createdAt; //创建时间

    private Date updatedAt; //更新时间

    private String updater; //创建者

    private boolean archive; //是否历史存档

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getDescription() {
        return description;
    }
    public void setDescription(String description) {
        this.description = description;
    }
    public String getState() {
        return state;
    }
    public void setState(String state) {
        this.state = state;
    }
    public Date getCreatedAt() {
        return createdAt;
    }
    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }
    public Date getUpdatedAt() {
        return updatedAt;
    }
    public void setUpdatedAt(Date updatedAt) {
        this.updatedAt = updatedAt;
    }
    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }
    public String getPlatform() {
        return platform;
    }
    public void setPlatform(String platform) {
        this.platform = platform;
    }
    public long getId() {
        return id;
    }
    public void setId(long id) {
        this.id = id;
    }
    public String getTestId() {
        return testId;
    }
    public void setTestId(String testId) {
        this.testId = testId;
    }
    public List<String> getTargets() {
        return targets;
    }
    public void setTargets(List<String> targets) {
        this.targets = targets;
    }
    public List<String> getExcludes() {
        return excludes;
    }
    public void setExcludes(List<String> excludes) {
        this.excludes = excludes;
    }
    public List<TestGroup> getTestGroups() {
        return testGroups;
    }
    public void setTestGroups(List<TestGroup> testGroups) {
        this.testGroups = testGroups;
    }
    public int getRatio() {
        return ratio;
    }
    public void setRatio(int ratio) {
        this.ratio = ratio;
    }
    public String getUpdater() {
        return updater;
    }
    public void setUpdater(String updater) {
        this.updater = updater;
    }
    public boolean isArchive() {
        return archive;
    }
    public void setArchive(boolean archive) {
        this.archive = archive;
    }
    public float getRatioPercent(){
        return ratio / 100f;
    }
    public void setRatioPercent(float percent){
        this.ratio = (int) (percent * 100);
    }
}
