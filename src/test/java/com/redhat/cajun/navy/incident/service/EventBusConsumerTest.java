package com.redhat.cajun.navy.incident.service;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonNodePresent;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartEquals;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import javax.enterprise.inject.Any;
import javax.inject.Inject;

import com.redhat.cajun.navy.incident.model.Incident;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.reactive.messaging.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.connectors.InMemorySink;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecord;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;

@QuarkusTest
public class EventBusConsumerTest {

    @InjectMock
    IncidentService incidentService;

    @Inject
    EventBusConsumer eventBusConsumer;

    @Inject @Any
    InMemoryConnector connector;

    @Captor
    ArgumentCaptor<Incident> incidentCaptor;

    boolean messageReplyCalled;

    Object messageReplyBody = null;

    boolean messageFailed;

    String failureMessage;

    @BeforeEach
    void init() {
        initMocks(this);
        messageReplyCalled = false;
        messageReplyBody = null;
        messageFailed = false;
        failureMessage = null;
        connector.sink("incident-event").clear();
    }

    @Test
    void testUnsupportedOperation() {
        Message<JsonObject> message = buildMessage(new JsonObject(), Collections.singletonMap("action", "unknown"));
        eventBusConsumer.consume(message);

        assertThat(messageFailed, equalTo(true));
        assertThat(failureMessage, equalTo("Unsupported operation"));

    }

    @Test
    void testIncidents() {

        Incident incident1 = new Incident.Builder("incident1")
                .lat("31.12345")
                .lon("-71.98765")
                .numberOfPeople(4)
                .medicalNeeded(true)
                .victimName("John Doe")
                .victimPhoneNumber("(211) 456-78990")
                .timestamp(Instant.now().toEpochMilli())
                .status("REPORTED")
                .build();

        Incident incident2 = new Incident.Builder("incident2")
                .lat("32.12345")
                .lon("-72.98765")
                .numberOfPeople(5)
                .medicalNeeded(true)
                .victimName("John Foo")
                .victimPhoneNumber("(111) 456-78990")
                .timestamp(Instant.now().toEpochMilli())
                .status("ASSIGNED")
                .build();

        when(incidentService.incidents()).thenReturn(Arrays.asList(incident1, incident2));

        Message<JsonObject> message = buildMessage(new JsonObject(), Collections.singletonMap("action", "incidents"));
        eventBusConsumer.consume(message);

        assertThat(messageReplyCalled, equalTo(true));
        assertThat(messageReplyBody, notNullValue());
        assertThat(messageReplyBody, isA(JsonObject.class));
        JsonObject body = (JsonObject) messageReplyBody;
        assertThat(body.containsKey("incidents"), equalTo(true));
        assertThat(body.getValue("incidents"), isA(JsonArray.class));
        JsonArray array = body.getJsonArray("incidents");
        assertThat(array.size(), equalTo(2));
        assertThat(array.getJsonObject(0).getString("id"), anyOf(equalTo("incident1"), equalTo("incident2")));
        assertThat(array.getJsonObject(1).getString("id"), anyOf(equalTo("incident1"), equalTo("incident2")));
        assertThat(array.getJsonObject(0).getString("id"), not(equalTo(array.getJsonObject(1).getString("id"))));
        JsonObject json = (JsonObject) array.stream().filter(o -> ((JsonObject) o).getString("id").equals("incident1")).findFirst().orElse(null);
        assertThat(json, notNullValue());
        assertThat(json.getString("lat"), equalTo(incident1.getLat()));
        assertThat(json.getString("lon"), equalTo(incident1.getLon()));
        assertThat(json.getBoolean("medicalNeeded"), equalTo(incident1.isMedicalNeeded()));
        assertThat(json.getInteger("numberOfPeople"), equalTo(incident1.getNumberOfPeople()));
        assertThat(json.getString("victimName"), equalTo(incident1.getVictimName()));
        assertThat(json.getString("victimPhoneNumber"), equalTo(incident1.getVictimPhoneNumber()));
        assertThat(json.getLong("timeStamp"), equalTo(incident1.getTimestamp()));
        assertThat(json.getString("status"), equalTo(incident1.getStatus()));
        verify(incidentService).incidents();
    }

    @Test
    void testIncidentsNotFound() {

        when(incidentService.incidents()).thenReturn(Collections.emptyList());

        Message<JsonObject> message = buildMessage(new JsonObject(), Collections.singletonMap("action", "incidents"));
        eventBusConsumer.consume(message);

        assertThat(messageReplyCalled, equalTo(true));
        assertThat(messageReplyBody, notNullValue());
        assertThat(messageReplyBody, isA(JsonObject.class));
        JsonObject body = (JsonObject) messageReplyBody;
        assertThat(body.containsKey("incidents"), equalTo(true));
        assertThat(body.getValue("incidents"), isA(JsonArray.class));
        JsonArray array = body.getJsonArray("incidents");
        assertThat(array.size(), equalTo(0));
    }

    @Test
    void testIncidentById() {

        Incident incident1 = new Incident.Builder("incident1")
                .lat("31.12345")
                .lon("-71.98765")
                .numberOfPeople(4)
                .medicalNeeded(true)
                .victimName("John Doe")
                .victimPhoneNumber("(211) 456-78990")
                .timestamp(Instant.now().toEpochMilli())
                .status("REPORTED")
                .build();

        when(incidentService.incidentByIncidentId("incident1")).thenReturn(incident1);

        Message<JsonObject> message = buildMessage(new JsonObject().put("incidentId", "incident1"), Collections.singletonMap("action", "incidentById"));
        eventBusConsumer.consume(message);

        assertThat(messageReplyCalled, equalTo(true));
        assertThat(messageReplyBody, notNullValue());
        assertThat(messageReplyBody, isA(JsonObject.class));
        JsonObject body = (JsonObject) messageReplyBody;
        assertThat(body.containsKey("incident"), equalTo(true));
        assertThat(body.getValue("incident"), isA(JsonObject.class));
        JsonObject json = body.getJsonObject("incident");
        assertThat(json, notNullValue());
        assertThat(json.getString("id"), equalTo(incident1.getId()));
        assertThat(json.getString("lat"), equalTo(incident1.getLat()));
        assertThat(json.getString("lon"), equalTo(incident1.getLon()));
        assertThat(json.getBoolean("medicalNeeded"), equalTo(incident1.isMedicalNeeded()));
        assertThat(json.getInteger("numberOfPeople"), equalTo(incident1.getNumberOfPeople()));
        assertThat(json.getString("victimName"), equalTo(incident1.getVictimName()));
        assertThat(json.getString("victimPhoneNumber"), equalTo(incident1.getVictimPhoneNumber()));
        assertThat(json.getLong("timeStamp"), equalTo(incident1.getTimestamp()));
        assertThat(json.getString("status"), equalTo(incident1.getStatus()));
        verify(incidentService).incidentByIncidentId("incident1");
    }

    @Test
    void testIncidentByIdNotFound() {

        when(incidentService.incidentByIncidentId("incident1")).thenReturn(null);

        Message<JsonObject> message = buildMessage(new JsonObject().put("incidentId", "incident1"), Collections.singletonMap("action", "incidentById"));
        eventBusConsumer.consume(message);

        assertThat(messageReplyCalled, equalTo(true));
        assertThat(messageReplyBody, notNullValue());
        assertThat(messageReplyBody, isA(JsonObject.class));
        JsonObject body = (JsonObject) messageReplyBody;
        assertThat(body.isEmpty(), equalTo(true));
    }

    @Test
    void testIncidentsByStatus() {

        Incident incident1 = new Incident.Builder("incident1")
                .lat("31.12345")
                .lon("-71.98765")
                .numberOfPeople(4)
                .medicalNeeded(true)
                .victimName("John Doe")
                .victimPhoneNumber("(211) 456-78990")
                .timestamp(Instant.now().toEpochMilli())
                .status("REPORTED")
                .build();

        Incident incident2 = new Incident.Builder("incident2")
                .lat("32.12345")
                .lon("-72.98765")
                .numberOfPeople(5)
                .medicalNeeded(true)
                .victimName("John Foo")
                .victimPhoneNumber("(111) 456-78990")
                .timestamp(Instant.now().toEpochMilli())
                .status("REPORTED")
                .build();

        when(incidentService.incidentsByStatus("REPORTED")).thenReturn(Arrays.asList(incident1, incident2));

        Message<JsonObject> message = buildMessage(new JsonObject().put("status", "REPORTED"), Collections.singletonMap("action", "incidentsByStatus"));
        eventBusConsumer.consume(message);

        assertThat(messageReplyCalled, equalTo(true));
        assertThat(messageReplyBody, notNullValue());
        assertThat(messageReplyBody, isA(JsonObject.class));
        JsonObject body = (JsonObject) messageReplyBody;
        assertThat(body.containsKey("incidents"), equalTo(true));
        assertThat(body.getValue("incidents"), isA(JsonArray.class));
        JsonArray array = body.getJsonArray("incidents");
        assertThat(array.size(), equalTo(2));
        assertThat(array.getJsonObject(0).getString("id"), anyOf(equalTo("incident1"), equalTo("incident2")));
        assertThat(array.getJsonObject(1).getString("id"), anyOf(equalTo("incident1"), equalTo("incident2")));
        assertThat(array.getJsonObject(0).getString("id"), not(equalTo(array.getJsonObject(1).getString("id"))));
        JsonObject json = (JsonObject) array.stream().filter(o -> ((JsonObject) o).getString("id").equals("incident1")).findFirst().orElse(null);
        assertThat(json, notNullValue());
        assertThat(json.getString("lat"), equalTo(incident1.getLat()));
        assertThat(json.getString("lon"), equalTo(incident1.getLon()));
        assertThat(json.getBoolean("medicalNeeded"), equalTo(incident1.isMedicalNeeded()));
        assertThat(json.getInteger("numberOfPeople"), equalTo(incident1.getNumberOfPeople()));
        assertThat(json.getString("victimName"), equalTo(incident1.getVictimName()));
        assertThat(json.getString("victimPhoneNumber"), equalTo(incident1.getVictimPhoneNumber()));
        assertThat(json.getLong("timeStamp"), equalTo(incident1.getTimestamp()));
        assertThat(json.getString("status"), equalTo(incident1.getStatus()));
        verify(incidentService).incidentsByStatus("REPORTED");
    }

    @Test
    void testIncidentsByStatusNotFound() {

        when(incidentService.incidentsByStatus("REPORTED")).thenReturn(Collections.emptyList());

        Message<JsonObject> message = buildMessage(new JsonObject().put("status", "REPORTED"), Collections.singletonMap("action", "incidentsByStatus"));
        eventBusConsumer.consume(message);

        assertThat(messageReplyCalled, equalTo(true));
        assertThat(messageReplyBody, notNullValue());
        assertThat(messageReplyBody, isA(JsonObject.class));
        JsonObject body = (JsonObject) messageReplyBody;
        assertThat(body.containsKey("incidents"), equalTo(true));
        assertThat(body.getValue("incidents"), isA(JsonArray.class));
        JsonArray array = body.getJsonArray("incidents");
        assertThat(array.size(), equalTo(0));
        verify(incidentService).incidentsByStatus("REPORTED");
    }

    @Test
    void testIncidentsByName() {

        Incident incident1 = new Incident.Builder("incident1")
                .lat("31.12345")
                .lon("-71.98765")
                .numberOfPeople(4)
                .medicalNeeded(true)
                .victimName("John Doe")
                .victimPhoneNumber("(211) 456-78990")
                .timestamp(Instant.now().toEpochMilli())
                .status("REPORTED")
                .build();

        Incident incident2 = new Incident.Builder("incident2")
                .lat("32.12345")
                .lon("-72.98765")
                .numberOfPeople(5)
                .medicalNeeded(true)
                .victimName("John Foo")
                .victimPhoneNumber("(111) 456-78990")
                .timestamp(Instant.now().toEpochMilli())
                .status("REPORTED")
                .build();

        when(incidentService.incidentsByVictimName("John%")).thenReturn(Arrays.asList(incident1, incident2));

        Message<JsonObject> message = buildMessage(new JsonObject().put("name", "John%"), Collections.singletonMap("action", "incidentsByName"));
        eventBusConsumer.consume(message);

        assertThat(messageReplyCalled, equalTo(true));
        assertThat(messageReplyBody, notNullValue());
        assertThat(messageReplyBody, isA(JsonObject.class));
        JsonObject body = (JsonObject) messageReplyBody;
        assertThat(body.containsKey("incidents"), equalTo(true));
        assertThat(body.getValue("incidents"), isA(JsonArray.class));
        JsonArray array = body.getJsonArray("incidents");
        assertThat(array.size(), equalTo(2));
        assertThat(array.getJsonObject(0).getString("id"), anyOf(equalTo("incident1"), equalTo("incident2")));
        assertThat(array.getJsonObject(1).getString("id"), anyOf(equalTo("incident1"), equalTo("incident2")));
        assertThat(array.getJsonObject(0).getString("id"), not(equalTo(array.getJsonObject(1).getString("id"))));
        JsonObject json = (JsonObject) array.stream().filter(o -> ((JsonObject) o).getString("id").equals("incident1")).findFirst().orElse(null);
        assertThat(json, notNullValue());
        assertThat(json.getString("lat"), equalTo(incident1.getLat()));
        assertThat(json.getString("lon"), equalTo(incident1.getLon()));
        assertThat(json.getBoolean("medicalNeeded"), equalTo(incident1.isMedicalNeeded()));
        assertThat(json.getInteger("numberOfPeople"), equalTo(incident1.getNumberOfPeople()));
        assertThat(json.getString("victimName"), equalTo(incident1.getVictimName()));
        assertThat(json.getString("victimPhoneNumber"), equalTo(incident1.getVictimPhoneNumber()));
        assertThat(json.getLong("timeStamp"), equalTo(incident1.getTimestamp()));
        assertThat(json.getString("status"), equalTo(incident1.getStatus()));
        verify(incidentService).incidentsByVictimName("John%");
    }

    @Test
    void testIncidentsByNameNotFound() {

        when(incidentService.incidentsByVictimName("John%")).thenReturn(Collections.emptyList());

        Message<JsonObject> message = buildMessage(new JsonObject().put("name", "John%"), Collections.singletonMap("action", "incidentsByName"));
        eventBusConsumer.consume(message);

        assertThat(messageReplyCalled, equalTo(true));
        assertThat(messageReplyBody, notNullValue());
        assertThat(messageReplyBody, isA(JsonObject.class));
        JsonObject body = (JsonObject) messageReplyBody;
        assertThat(body.containsKey("incidents"), equalTo(true));
        assertThat(body.getValue("incidents"), isA(JsonArray.class));
        JsonArray array = body.getJsonArray("incidents");
        assertThat(array.size(), equalTo(0));
        verify(incidentService).incidentsByVictimName("John%");
    }

    @Test
    void testCreateIncident() {

        Incident incident1 = new Incident.Builder("incident1")
                .lat("30.12345")
                .lon("-70.98765")
                .numberOfPeople(3)
                .medicalNeeded(true)
                .victimName("John Doe")
                .victimPhoneNumber("(111) 123-45678")
                .timestamp(Instant.now().toEpochMilli())
                .status("REPORTED")
                .build();

        JsonObject toCreate = new JsonObject()
                .put("lat", new BigDecimal("30.12345").doubleValue())
                .put("lon", new BigDecimal("-70.98765").doubleValue())
                .put("numberOfPeople", 3)
                .put("medicalNeeded", true)
                .put("victimName", "John Doe")
                .put("victimPhoneNumber", "(111) 123-45678");

        when(incidentService.create(Mockito.any(Incident.class))).thenReturn(incident1);

        InMemorySink<String> results = connector.sink("incident-event");

        Message<JsonObject> message = buildMessage(toCreate, Collections.singletonMap("action", "createIncident"));
        eventBusConsumer.consume(message);

        assertThat(messageReplyCalled, equalTo(true));
        assertThat(messageReplyBody, notNullValue());
        assertThat(messageReplyBody, isA(JsonObject.class));
        JsonObject body = (JsonObject) messageReplyBody;
        assertThat(body.isEmpty(), equalTo(true));

        assertThat(results.received().size(), equalTo(1));
        org.eclipse.microprofile.reactive.messaging.Message<String> record = results.received().get(0);
        assertThat(record, instanceOf(OutgoingKafkaRecord.class));
        String value = record.getPayload();
        String key = ((OutgoingKafkaRecord<String, String>)record).getKey();
        assertThat(key, equalTo(incident1.getId()));
        assertThat(value, jsonNodePresent("id"));
        assertThat(value, jsonPartEquals("messageType", "IncidentReportedEvent"));
        assertThat(value, jsonPartEquals("invokingService", "IncidentService"));
        assertThat(value, jsonNodePresent("timestamp"));
        assertThat(value, jsonNodePresent("body"));
        assertThat(value, jsonPartEquals("body.id", incident1.getId()));
        assertThat(value, jsonPartEquals("body.lat", new BigDecimal(incident1.getLat()).doubleValue()));
        assertThat(value, jsonPartEquals("body.lon", new BigDecimal(incident1.getLon()).doubleValue()));
        assertThat(value, jsonPartEquals("body.medicalNeeded", incident1.isMedicalNeeded()));
        assertThat(value, jsonPartEquals("body.numberOfPeople", incident1.getNumberOfPeople()));
        assertThat(value, jsonPartEquals("body.victimName", incident1.getVictimName()));
        assertThat(value, jsonPartEquals("body.victimPhoneNumber", incident1.getVictimPhoneNumber()));
        assertThat(value, jsonPartEquals("body.status", incident1.getStatus()));
        assertThat(value, jsonPartEquals("body.timestamp", incident1.getTimestamp()));
        verify(incidentService).create(incidentCaptor.capture());
        Incident captured = incidentCaptor.getValue();
        assertThat(captured, notNullValue());
        assertThat(captured.getLat(), equalTo("30.12345"));
        assertThat(captured.getLon(), equalTo("-70.98765"));
        assertThat(captured.getNumberOfPeople(), equalTo(3));
        assertThat(captured.isMedicalNeeded(), equalTo(true));
        assertThat(captured.getVictimName(), equalTo("John Doe"));
        assertThat(captured.getVictimPhoneNumber(), equalTo("(111) 123-45678"));
        assertThat(captured.getId(), nullValue());
        assertThat(captured.getStatus(), nullValue());
    }

    @Test
    void testReset() {
        Message<JsonObject> message = buildMessage(new JsonObject(), Collections.singletonMap("action", "reset"));
        eventBusConsumer.consume(message);

        assertThat(messageReplyCalled, equalTo(true));
        assertThat(messageReplyBody, notNullValue());
        assertThat(messageReplyBody, isA(JsonObject.class));
        JsonObject body = (JsonObject) messageReplyBody;
        assertThat(body.isEmpty(), equalTo(true));
        verify(incidentService).reset();

    }

    private Message<JsonObject> buildMessage(JsonObject body, Map<String, String> headers) {
        MessageImpl<JsonObject, JsonObject> message = new MessageImpl<>(body);
        headers.forEach((key, value) -> message.headers().add(key, value));
        return new Message<>(message);
    }

    private class MessageImpl<U, V> extends io.vertx.core.eventbus.impl.MessageImpl<U, V> {

        public MessageImpl(V body) {
            this.receivedBody = body;
        }

        @Override
        public void reply(Object message) {
            EventBusConsumerTest.this.messageReplyCalled = true;
            EventBusConsumerTest.this.messageReplyBody = message;
        }

        @Override
        public void fail(int failureCode, String message) {
            EventBusConsumerTest.this.messageFailed = true;
            EventBusConsumerTest.this.failureMessage = message;
        }
    }


}
