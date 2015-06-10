/*
 * Copyright 2014 Bicc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.bicc.kafkameter;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import org.apache.jorphan.logging.LoggingManager;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

/**
 * A {@link org.apache.jmeter.samplers.Sampler Sampler} which produces Kafka messages.
 * Based on "https://github.com/BrightTag/kafkameter" project, but with new Kafka API implemented since 0.8.2.0 ver.
 *
 * @author EugeneYushin
 * @see "http://ilkinbalkanay.blogspot.com/2010/03/load-test-whatever-you-want-with-apache.html"
 * @see "http://newspaint.wordpress.com/2012/11/28/creating-a-java-sampler-for-jmeter/"
 * @see "http://jmeter.512774.n5.nabble.com/Custom-Sampler-Tutorial-td4490189.html"
 * @since 2015-06-05
 */
public class NewApiKafkaProducerSampler extends AbstractJavaSamplerClient {

    private static final Logger log = LoggingManager.getLoggerForClass();

    /**
     * Parameter for setting the Kafka topic name.
     */
    private static final String PARAMETER_KAFKA_TOPIC = "kafka_topic";

    /**
     * Parameter for setting the Kafka key.
     */
    //private static final String PARAMETER_KAFKA_KEY = "kafka_key";

    /**
     * Parameter for setting the Kafka message.
     */
    private static final String PARAMETER_KAFKA_MESSAGE = "kafka_message";

    /**
     * Parameter for setting Kafka's {@code key.serializer} property.
     */
    private static final String PARAMETER_KAFKA_KEY_SERIALIZER = "kafka_key_serializer";

    /**
     * Parameter for setting Kafka's {@code value.serializer} property.
     */
    private static final String PARAMETER_KAFKA_VALUE_SERIALIZER = "kafka_value_serializer";

    private KafkaProducer producer;

    @Override
    public void setupTest(JavaSamplerContext context) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, context.getParameter(PARAMETER_KAFKA_KEY_SERIALIZER));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, context.getParameter(PARAMETER_KAFKA_VALUE_SERIALIZER));
        // Hardcoded version
        // props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.RETRIES_CONFIG, "1");


        //producer = new KafkaProducer(props, new ByteArraySerializer(), new ByteArraySerializer());
        producer = new KafkaProducer<String, byte[]>(props);

        // Example
        // Properties props = new Properties();
        // props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "server1:123,server2:456");
        // props.put(ProducerConfig.RETRIES_CONFIG, "3");
        // props.put(ProducerConfig.ACKS_CONFIG, "all");
        // props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        // props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
        // props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
        // props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        //
        // KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        producer.close();
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument(PARAMETER_KAFKA_TOPIC, "${PARAMETER_KAFKA_TOPIC}");
        //defaultParameters.addArgument(PARAMETER_KAFKA_KEY, "${PARAMETER_KAFKA_KEY}");
        defaultParameters.addArgument(PARAMETER_KAFKA_MESSAGE, "${PARAMETER_KAFKA_MESSAGE}");
        defaultParameters.addArgument(PARAMETER_KAFKA_KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
        defaultParameters.addArgument(PARAMETER_KAFKA_VALUE_SERIALIZER, "org.apache.kafka.common.serialization.ByteArraySerializer");
        return defaultParameters;
    }


    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult result = newSampleResult();
        String topic = context.getParameter(PARAMETER_KAFKA_TOPIC);
        //Long key = context.getLongParameter(PARAMETER_KAFKA_KEY);
        String message = context.getParameter(PARAMETER_KAFKA_MESSAGE);
        sampleResultStart(result, message);
        try {
            //producer.send(new ProducerRecord(topic, key, message.getBytes()));
            producer.send(new ProducerRecord(topic, message.getBytes()));
            sampleResultSuccess(result, null);
        } catch (Exception e) {
            sampleResultFailed(result, "500", e);
        }
        return result;
    }

    /**
     * Use UTF-8 for encoding of strings
     */
    private static final String ENCODING = "UTF-8";

    /**
     * Factory for creating new {@link SampleResult}s.
     */
    private SampleResult newSampleResult() {
        SampleResult result = new SampleResult();
        result.setDataEncoding(ENCODING);
        result.setDataType(SampleResult.TEXT);
        return result;
    }

    /**
     * Start the sample request and set the {@code samplerData} to {@code data}.
     *
     * @param result the sample result to update
     * @param data   the request to set as {@code samplerData}
     */
    private void sampleResultStart(SampleResult result, String data) {
        result.setSamplerData(data);
        result.sampleStart();
    }

    /**
     * Mark the sample result as {@code end}ed and {@code successful} with an "OK" {@code responseCode},
     * and if the response is not {@code null} then set the {@code responseData} to {@code response},
     * otherwise it is marked as not requiring a response.
     *
     * @param result   sample result to change
     * @param response the successful result message, may be null.
     */
    private void sampleResultSuccess(SampleResult result, /* @Nullable */ String response) {
        result.sampleEnd();
        result.setSuccessful(true);
        result.setResponseCodeOK();
        if (response != null) {
            result.setResponseData(response, ENCODING);
        } else {
            result.setResponseData("No response required", ENCODING);
        }
    }

    /**
     * Mark the sample result as @{code end}ed and not {@code successful}, and set the
     * {@code responseCode} to {@code reason}.
     *
     * @param result the sample result to change
     * @param reason the failure reason
     */
    private void sampleResultFailed(SampleResult result, String reason) {
        result.sampleEnd();
        result.setSuccessful(false);
        result.setResponseCode(reason);
    }

    /**
     * Mark the sample result as @{code end}ed and not {@code successful}, set the
     * {@code responseCode} to {@code reason}, and set {@code responseData} to the stack trace.
     *
     * @param result    the sample result to change
     * @param exception the failure exception
     */
    private void sampleResultFailed(SampleResult result, String reason, Exception exception) {
        sampleResultFailed(result, reason);
        result.setResponseMessage("Exception: " + exception);
        result.setResponseData(getStackTrace(exception), ENCODING);
    }

    /**
     * Return the stack trace as a string.
     *
     * @param exception the exception containing the stack trace
     * @return the stack trace
     */
    private String getStackTrace(Exception exception) {
        StringWriter stringWriter = new StringWriter();
        exception.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }
}
