package com.illiahostiuk.lab4.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PopularStationResult {

    private String date;

    private String station;

    private Long trips;
}