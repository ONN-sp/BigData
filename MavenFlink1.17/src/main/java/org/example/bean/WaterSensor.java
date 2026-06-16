package org.example.bean;

public class WaterSensor {
    private String id;
    private Long timestamp;
    private Integer value;

    public WaterSensor() {
    }

    public WaterSensor(String id, Long timestamp, Integer value) {
        this.id = id;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }
}
