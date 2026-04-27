/*
 * Copyright 2015 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The "Popular Places" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to identify every five minutes popular areas where many taxi rides
 * arrived or departed in the last 15 minutes.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class PopularPlacesExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);
		final int popThreshold = params.getInt("threshold", 20);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiRide> rides =
				env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> popularPlaces = rides
				.filter(new NYCFilter())
				.map(new GridCellMatcher())
				.keyBy(0, 1)
				.timeWindow(Time.minutes(15), Time.minutes(5))
				.apply(new RideCounter())
				.filter(value -> value.f3 >= popThreshold)
				.map(new GridToCoordinates());

		printOrTest(popularPlaces);

		env.execute("Popular Places");
	}

	/**
	 * Map taxi ride to grid cell and event type.
	 * Start records use departure location, end record use arrival location.
	 */
	public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple2<Integer, Boolean>> {

		@Override
		public Tuple2<Integer, Boolean> map(TaxiRide taxiRide) throws Exception {
			if (taxiRide.isStart) {
				int gridId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
				return new Tuple2<>(gridId, true);
			} else {
				int gridId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
				return new Tuple2<>(gridId, false);
			}
		}
	}
	public static class RideCounter implements WindowFunction<
			Tuple2<Integer, Boolean>,
			Tuple4<Integer, Long, Boolean, Integer>,
			Tuple,
			TimeWindow> {

		@Override
		public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Tuple2<Integer, Boolean>> values,
				Collector<Tuple4<Integer, Long, Boolean, Integer>> out) throws Exception {

			int cellId = ((Tuple2<Integer, Boolean>) key).f0;
			boolean isStart = ((Tuple2<Integer, Boolean>) key).f1;
			long windowEnd = window.getEnd();

			int count = 0;
			for (Tuple2<Integer, Boolean> ignored : values) {
				count++;
			}

			out.collect(new Tuple4<>(cellId, windowEnd, isStart, count));
		}
	}

	public static class GridToCoordinates implements MapFunction<
			Tuple4<Integer, Long, Boolean, Integer>,
			Tuple5<Float, Float, Long, Boolean, Integer>> {

		@Override
		public Tuple5<Float, Float, Long, Boolean, Integer> map(
				Tuple4<Integer, Long, Boolean, Integer> cellCount) throws Exception {

			return new Tuple5<>(
					GeoUtils.getGridCellCenterLon(cellCount.f0),
					GeoUtils.getGridCellCenterLat(cellCount.f0),
					cellCount.f1,
					cellCount.f2,
					cellCount.f3
			);
		}
	}



	public static class NYCFilter implements FilterFunction<TaxiRide> {
		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}
}
