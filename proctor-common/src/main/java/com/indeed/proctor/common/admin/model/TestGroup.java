package com.indeed.proctor.common.admin.model;

public class TestGroup {

    private long id;//自增id
    private String name;//分组名称
    private String description;//实验描述
    private String variable; //变量值
    private int ratio; //万分之x 1-1000

    private Test test;//对应的test的id
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
    public String getVariable() {
        return variable;
    }
    public void setVariable(String variable) {
        this.variable = variable;
    }
    public long getId() {
        return id;
    }
    public void setId(long id) {
        this.id = id;
    }
    public Test getTest() {
        return test;
    }
    public void setTest(Test test) {
        this.test = test;
    }
    public int getRatio() {
        return ratio;
    }
    public void setRatio(int ratio) {
        this.ratio = ratio;
    }
}
