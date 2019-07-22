package com.practice.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	String consumerKey = "9GdtFeVAVaFCqQApK8LYDpkW0";
	String consumerSecret = "3PYDpCurmGk6XxzB3oTmk7wROUNkPyiS5uSUKbMwUJSmnbXQTP";

	String tokenKey = "4555934249-2FU6MDU0BRGxBbVKPDQqY4IAVRjD0sO0YkZlgNs";
	String tokenSecret = "2DmyRIAryHwvgOLTYkS3zzeV8iAgDEkyBgFFPVlKbYU0f";

	public TwitterProducer() {

	}

	public static void main(String[] args) {
		new TwitterProducer().run();

	}

	public void run() {
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		Client client = createTwitterClient(msgQueue);
		client.connect();

		KafkaProducer<String, String> producer = createKafkaProducer();

		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				client.stop();
			}

			if (msg != null) {
				producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if (e != null) {
							System.out.println("Something bad happened");
						}

					}
				});
			}
		}
		System.out.println("End of applciation");
	}

	private KafkaProducer<String, String> createKafkaProducer() {
		String bootStrapServer = "127.0.0.1:9092";

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
//		List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("bitcoin");
//		hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, tokenKey, tokenSecret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue)); // optional: use this
																	// if you want to
																	// process client
																	// events

		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}

}
