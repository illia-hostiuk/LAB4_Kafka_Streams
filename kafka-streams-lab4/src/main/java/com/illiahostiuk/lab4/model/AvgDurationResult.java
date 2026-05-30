package com.illiahostiuk.lab4.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AvgDurationResult {

    private String date;

    private double averageDuration;
}