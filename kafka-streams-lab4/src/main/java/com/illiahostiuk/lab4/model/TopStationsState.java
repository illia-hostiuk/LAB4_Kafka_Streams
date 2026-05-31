package com.illiahostiuk.lab4.model;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class TopStationsState {

    private Map<String, Long> stations =
            new HashMap<>();
}