package com.illiahostiuk.lab4.stream;

import org.apache.kafka.streams.KeyValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.illiahostiuk.lab4.model.AvgDurationResult;
import com.illiahostiuk.lab4.model.TripsCountResult;
import com.illiahostiuk.lab4.model.DurationStats;
import com.illiahostiuk.lab4.model.StationCount;
import com.illiahostiuk.lab4.model.PopularStationResult;
import com.illiahostiuk.lab4.model.TopStationsResult;
import com.illiahostiuk.lab4.model.TopStationsState;
import com.illiahostiuk.lab4.model.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class TripStreamConfig {

    @Bean
    public KStream<String, String> tripStream(StreamsBuilder builder) {

        ObjectMapper mapper = new ObjectMapper();

        JsonSerde<DurationStats> statsSerde =
                new JsonSerde<>(DurationStats.class);

        JsonSerde<AvgDurationResult> resultSerde =
                new JsonSerde<>(AvgDurationResult.class);

        JsonSerde<TripsCountResult> countSerde =
                new JsonSerde<>(TripsCountResult.class);

        JsonSerde<StationCount> stationSerde =
                new JsonSerde<>(StationCount.class);

        JsonSerde<PopularStationResult> popularSerde =
                new JsonSerde<>(PopularStationResult.class);

        JsonSerde<TopStationsResult> top3Serde =
                new JsonSerde<>(TopStationsResult.class);

        JsonSerde<TopStationsState> topStateSerde =
                new JsonSerde<>(TopStationsState.class);

        KStream<String, String> source = builder.stream("topic1");

        KTable<String, DurationStats> statsTable = source
                .map((key, value) -> {
                    try {

                        Trip trip = mapper.readValue(value, Trip.class);

                        String date =
                                trip.getStart_time().substring(0, 10);

                        double duration =
                                Double.parseDouble(
                                        trip.getTripduration()
                                                .replace(",", "")
                                );

                        return KeyValue.pair(
                                date,
                                new DurationStats(duration, 1)
                        );

                    } catch (Exception e) {
                        return KeyValue.pair(
                                "ERROR",
                                new DurationStats(0, 0)
                        );
                    }
                })
                .filter((key, value) ->
                        !"ERROR".equals(key))
                .groupByKey(
                        Grouped.with(
                                Serdes.String(),
                                statsSerde
                        )
                )
                .reduce(
                        (oldValue, newValue) ->
                                new DurationStats(
                                        oldValue.getTotalDuration()
                                                + newValue.getTotalDuration(),
                                        oldValue.getCount()
                                                + newValue.getCount()
                                ),
                        Materialized.with(
                                Serdes.String(),
                                statsSerde
                        )
                );

        KTable<String, Long> tripsCountTable = source
                .map((key, value) -> {
                    try {

                        Trip trip = mapper.readValue(value, Trip.class);

                        String date =
                                trip.getStart_time().substring(0, 10);

                        return KeyValue.pair(date, date);

                    } catch (Exception e) {

                        return KeyValue.pair("ERROR", "ERROR");
                    }
                })
                .filter((key, value) ->
                        !"ERROR".equals(key))
                .groupByKey()
                .count();

        KTable<String, Long> stationCountTable = source
                .map((key, value) -> {
                    try {

                        Trip trip = mapper.readValue(value, Trip.class);

                        String date =
                                trip.getStart_time().substring(0, 10);

                        return KeyValue.pair(
                                date + "|" + trip.getFrom_station_name(),
                                1L
                        );

                    } catch (Exception e) {

                        return KeyValue.pair("ERROR", 0L);
                    }
                })
                .filter((key, value) ->
                        !"ERROR".equals(key))
                .groupByKey(
                        Grouped.with(
                                Serdes.String(),
                                Serdes.Long()
                        )
                )
                .count(
                        Materialized.with(
                                Serdes.String(),
                                Serdes.Long()
                        )
                );

        KTable<String, StationCount> popularStationTable =
                stationCountTable
                        .toStream()
                        .map((key, count) -> {

                            String[] parts = key.split("\\|", 2);

                            String date = parts[0];
                            String station = parts[1];

                            return KeyValue.pair(
                                    date,
                                    new StationCount(
                                            station,
                                            count
                                    )
                            );
                        })
                        .groupByKey(
                                Grouped.with(
                                        Serdes.String(),
                                        stationSerde
                                )
                        )
                        .reduce(
                                (oldValue, newValue) ->
                                        newValue.getCount() > oldValue.getCount()
                                                ? newValue
                                                : oldValue,
                                Materialized.with(
                                        Serdes.String(),
                                        stationSerde
                                )
                        );
        KTable<String, Long> allStationsTable = source
                .flatMap((key, value) -> {

                    try {

                        Trip trip = mapper.readValue(value, Trip.class);

                        String date =
                                trip.getStart_time().substring(0, 10);

                        return java.util.List.of(
                                KeyValue.pair(
                                        date + "|" + trip.getFrom_station_name(),
                                        1L
                                ),
                                KeyValue.pair(
                                        date + "|" + trip.getTo_station_name(),
                                        1L
                                )
                        );

                    } catch (Exception e) {

                        return java.util.Collections.emptyList();
                    }
                })
                .groupByKey(
                        Grouped.with(
                                Serdes.String(),
                                Serdes.Long()
                        )
                )
                .count(
                        Materialized.with(
                                Serdes.String(),
                                Serdes.Long()
                        )
                );

        KTable<String, TopStationsState> top3Table =
                allStationsTable
                        .toStream()
                        .map((key, count) -> {

                            String[] parts =
                                    key.split("\\|", 2);

                            return KeyValue.pair(
                                    parts[0],
                                    new StationCount(
                                            parts[1],
                                            count
                                    )
                            );
                        })
                        .groupByKey(
                                Grouped.with(
                                        Serdes.String(),
                                        stationSerde
                                )
                        )
                        .aggregate(
                                TopStationsState::new,

                                (date, stationCount, state) -> {

                                    state.getStations().put(
                                            stationCount.getStation(),
                                            stationCount.getCount()
                                    );

                                    return state;
                                },

                                Materialized.with(
                                        Serdes.String(),
                                        topStateSerde
                                )
                        );

        statsTable
                .toStream()
                .map((date, stats) ->
                        KeyValue.pair(
                                date,
                                new AvgDurationResult(
                                        date,
                                        stats.average()
                                )
                        )
                )
                .to(
                        "avg-duration-topic-test3",
                        Produced.with(
                                Serdes.String(),
                                resultSerde
                        )
                );
        tripsCountTable
                .toStream()
                .map((date, count) ->
                        KeyValue.pair(
                                date,
                                new TripsCountResult(
                                        date,
                                        count
                                )
                        )
                )
                .to(
                        "trips-count-topic",
                        Produced.with(
                                Serdes.String(),
                                countSerde
                        )
                );
        stationCountTable
                .toStream()
                .to(
                        "station-count-topic",
                        Produced.with(
                                Serdes.String(),
                                Serdes.Long()
                        )
                );

        popularStationTable
                .toStream()
                .map((date, stationCount) ->
                        KeyValue.pair(
                                date,
                                new PopularStationResult(
                                        date,
                                        stationCount.getStation(),
                                        stationCount.getCount()
                                )
                        )
                )
                .to(
                        "popular-station-topic",
                        Produced.with(
                                Serdes.String(),
                                popularSerde
                        )
                );

        top3Table
                .toStream()
                .map((date, state) -> {

                    java.util.List<String> top3 =
                            state.getStations()
                                    .entrySet()
                                    .stream()
                                    .sorted(
                                            (a, b) ->
                                                    Long.compare(
                                                            b.getValue(),
                                                            a.getValue()
                                                    )
                                    )
                                    .limit(3)
                                    .map(java.util.Map.Entry::getKey)
                                    .toList();

                    return KeyValue.pair(
                            date,
                            new TopStationsResult(
                                    date,
                                    top3
                            )
                    );
                })
                .to(
                        "top3-stations-topic",
                        Produced.with(
                                Serdes.String(),
                                top3Serde
                        )
                );
        return source;
    }
}