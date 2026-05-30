package com.illiahostiuk.lab4.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class Trip {

    private Long trip_id;

    private String start_time;

    private String end_time;

    private String tripduration;

    private String from_station_name;

    private String to_station_name;
}