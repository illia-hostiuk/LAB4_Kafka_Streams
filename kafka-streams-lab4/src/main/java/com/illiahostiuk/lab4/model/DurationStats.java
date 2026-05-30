package com.illiahostiuk.lab4.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DurationStats {

    private double totalDuration;

    private long count;

    public DurationStats add(double duration) {
        this.totalDuration += duration;
        this.count++;
        return this;
    }

    public double average() {
        return count == 0 ? 0 : totalDuration / count;
    }
}