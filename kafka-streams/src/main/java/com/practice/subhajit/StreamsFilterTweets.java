package com.practice.subhajit;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class StreamsFilterTweets {

	public static void main(String[] args) {
		String bootStrapServer = "127.0.0.1:9092";
		String groupId = "my-second-application";

		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "dem-kafka-streams");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

		StreamsBuilder streamsBuilder = new StreamsBuilder();

		KStream<String, String> inputTopic = streamsBuilder.stream("twitter_topics");
		KStream<String, String> filteredStreams = inputTopic.filter((k, v) -> extract(v) > 10);

		filteredStreams.to("important_tweets");

		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

		kafkaStreams.start();
	}

	private static JsonParser jsonParser = new JsonParser();

	private static Integer extract(String tweetJson) {

		try {
			return jsonParser.parse(tweetJson).getAsJsonObject().get("user").getAsJsonObject().get("follower_count")
					.getAsInt();
		} catch (NullPointerException e) {
			return 0;
		}
	}

}
