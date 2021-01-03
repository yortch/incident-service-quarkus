package com.redhat.emergency.response.incident.consumer;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import javax.enterprise.inject.Any;
import javax.inject.Inject;

import com.redhat.emergency.response.incident.service.IncidentService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.AsyncResultUni;
import io.smallrye.reactive.messaging.ce.impl.DefaultOutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.connectors.InMemorySink;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerImpl;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import io.vertx.kafka.client.consumer.impl.KafkaReadStreamImpl;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

@QuarkusTest
public class IncidentCommandMessageSourceTest {

    @InjectMock
    IncidentService incidentService;

    @Inject
    IncidentCommandMessageSource source;

    @Captor
    ArgumentCaptor<JsonObject> jsonObjectCaptor;

    @Inject @Any
    InMemoryConnector connector;

    private boolean messageAck = false;

    @BeforeEach
    void init() {
        openMocks(this);
        messageAck = false;
        connector.sink("incident-event").clear();
    }

    @Test
    void testProcessUpdateIncidentCommand() throws ExecutionException, InterruptedException {

        String json = "{" +
                "\"incident\" : {" +
                "\"id\" : \"incident1\"," +
                "\"status\" : \"ASSIGNED\"" +
                "} " +
                "}";

        JsonObject updated = new JsonObject()
                .put("id", "incident1")
                .put("lat", 30.12345)
                .put("lon", -70.98765)
                .put("numberOfPeople", 3)
                .put("medicalNeeded", true)
                .put("victimName", "John Doe")
                .put("victimPhoneNumber", "(111) 123-45678")
                .put("timestamp", Instant.now().toEpochMilli())
                .put("status", "ASSIGNED");

        when(incidentService.updateIncident(any(JsonObject.class))).thenReturn(updated);
        InMemorySink<String> results = connector.sink("incident-event");

        CompletionStage<CompletionStage<Void>> c = source.processMessage(toRecord("incident1", json, true, "UpdateIncidentCommand"));
        c.toCompletableFuture().get();

        verify(incidentService).updateIncident(jsonObjectCaptor.capture());
        JsonObject toUpdate = jsonObjectCaptor.getValue();
        assertThat(toUpdate, notNullValue());
        assertThat(toUpdate.getString("id"), equalTo("incident1"));
        assertThat(toUpdate.getString("status"), equalTo("ASSIGNED"));
        assertThat(toUpdate.getLong("lat"), nullValue());

        assertThat(messageAck, equalTo(true));

        assertThat(results.received().size(), equalTo(1));
        org.eclipse.microprofile.reactive.messaging.Message<String> record = results.received().get(0);
        String value = record.getPayload();
        assertThat(value, jsonPartEquals("id", updated.getString("id")));
        assertThat(value, jsonPartEquals("lat", BigDecimal.valueOf(updated.getDouble("lat"))));
        assertThat(value, jsonPartEquals("lon", BigDecimal.valueOf(updated.getDouble("lon"))));
        assertThat(value, jsonPartEquals("medicalNeeded", updated.getBoolean("medicalNeeded")));
        assertThat(value, jsonPartEquals("numberOfPeople", updated.getInteger("numberOfPeople")));
        assertThat(value, jsonPartEquals("victimName", updated.getString("victimName")));
        assertThat(value, jsonPartEquals("victimPhoneNumber", updated.getString("victimPhoneNumber")));
        assertThat(value, jsonPartEquals("status", updated.getString("status")));
        assertThat(value, jsonPartEquals("timestamp", updated.getLong("timestamp")));
        OutgoingKafkaRecordMetadata outgoingKafkaRecordMetadata = null;
        DefaultOutgoingCloudEventMetadata outgoingCloudEventMetadata = null;
        for (Object next : record.getMetadata()) {
            if (next instanceof OutgoingKafkaRecordMetadata) {
                outgoingKafkaRecordMetadata = (OutgoingKafkaRecordMetadata) next;
            } else if (next instanceof DefaultOutgoingCloudEventMetadata) {
                outgoingCloudEventMetadata = (DefaultOutgoingCloudEventMetadata) next;
            }
        }
        assertThat(outgoingCloudEventMetadata, notNullValue());
        assertThat(outgoingKafkaRecordMetadata, notNullValue());
        String key = (String) outgoingKafkaRecordMetadata.getKey();
        assertThat(key, equalTo(updated.getString("id")));
        assertThat(outgoingCloudEventMetadata.getId(), notNullValue());
        assertThat(outgoingCloudEventMetadata.getSpecVersion(), equalTo("1.0"));
        assertThat(outgoingCloudEventMetadata.getType(), equalTo("IncidentUpdatedEvent"));
        assertThat(outgoingCloudEventMetadata.getTimeStamp().isPresent(), is(true));
    }

    @Test
    public void testProcessMessageWrongMessageType() throws ExecutionException, InterruptedException {

        String json = "{}";

        InMemorySink<String> results = connector.sink("incident-event");

        CompletionStage<CompletionStage<Void>> c =  source.processMessage(toRecord("incident1", json, true, "WrongType"));
        c.toCompletableFuture().get();

        verify(incidentService, never()).updateIncident(any(JsonObject.class));
        assertThat(messageAck, equalTo(true));
        assertThat(results.received().size(), equalTo(0));
    }

    @Test
    public void testProcessMessageNotACloudEvent() throws ExecutionException, InterruptedException {

        String json = "{}";

        InMemorySink<String> results = connector.sink("incident-event");

        CompletionStage<CompletionStage<Void>> c =  source.processMessage(toRecord("incident1", json, false, "WrongType"));
        c.toCompletableFuture().get();

        verify(incidentService, never()).updateIncident(any(JsonObject.class));
        assertThat(messageAck, equalTo(true));
        assertThat(results.received().size(), equalTo(0));
    }

    private IncomingKafkaRecord<String, String> toRecord(String key, String payload, boolean cloudEvent, String type) {
        MockKafkaConsumer<String, String> mc = new MockKafkaConsumer<>();
        ConsumerRecord<String, String> cr;
        if (cloudEvent) {
            RecordHeaders headers = new RecordHeaders();
            headers.add("ce_specversion", "1.0".getBytes());
            headers.add("ce_id", "18cb49fe-9353-4856-9a0c-d66fe1237c86".getBytes());
            headers.add("ce_type", type.getBytes());
            headers.add("ce_source", "test".getBytes());
            headers.add("ce_time", "2020-12-30T19:54:20.765566GMT".getBytes());
            cr = new ConsumerRecord<>("topic", 1, 100, ConsumerRecord.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                    (long) ConsumerRecord.NULL_CHECKSUM, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, key, payload, headers);
        } else {
            cr = new ConsumerRecord<>("topic", 1, 100, key, payload);
        }
        KafkaConsumerRecord<String, String> kcr = new KafkaConsumerRecord<>(new KafkaConsumerRecordImpl<>(cr));
        KafkaCommitHandler kch = new KafkaCommitHandler() {
            @Override
            public <K, V> CompletionStage<Void> handle(IncomingKafkaRecord<K, V> record) {
                Uni<Void> uni = AsyncResultUni.toUni(mc::commit);
                return uni.subscribeAsCompletionStage();
            }
        };
        return new IncomingKafkaRecord<>(kcr, kch, null, true, false);
    }

    private class MockKafkaConsumer<K, V> extends KafkaConsumerImpl<K, V> {

        public MockKafkaConsumer() {
            super(new KafkaReadStreamImpl<>(null, null));
        }

        public MockKafkaConsumer(KafkaReadStream<K, V> stream) {
            super(stream);
        }

        @Override
        public void commit(Handler<AsyncResult<Void>> completionHandler) {
            IncidentCommandMessageSourceTest.this.messageAck = true;
            Promise<Void> future = Promise.promise();
            future.future().onComplete(completionHandler);
            future.complete(null);
        }
    }

}
