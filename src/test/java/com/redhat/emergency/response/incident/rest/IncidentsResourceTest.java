package com.redhat.emergency.response.incident.rest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import java.time.Instant;

import com.redhat.emergency.response.incident.service.EventBusConsumer;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.restassured.http.ContentType;
import io.restassured.http.Header;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.Message;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

@QuarkusTest
public class IncidentsResourceTest {

    @InjectMock
    EventBusConsumer eventBusConsumer;

    @Captor
    ArgumentCaptor<Message<JsonObject>> messageCaptor;

    @BeforeEach
    void init() {
        initMocks(this);
    }

    @Test
    void testReset() {

        doAnswer(invocation -> {
            Message<JsonObject> msg = invocation.getArgument(0);
            msg.replyAndForget(new JsonObject());
            return null;
        }).when(eventBusConsumer).consume(any(Message.class));

        given().when().post("/incidents/reset")
                .then().assertThat().statusCode(200).body(equalTo(""));
        verify(eventBusConsumer).consume(messageCaptor.capture());
        assertThat(messageCaptor.getValue().headers().get("action"), equalTo("reset"));
        assertThat(messageCaptor.getValue().body().isEmpty(), equalTo(true));
    }

    @Test
    void testIncidentById() {

        JsonObject incident = new JsonObject().put("id", "incident1")
                .put("lat", "30.12345")
                .put("lon", "-70.98765")
                .put("medicalNeeded", true)
                .put("numberOfPeople", 3)
                .put("victimName", "John Doe")
                .put("victimPhoneNumber", "(123) 456-7890)")
                .put("timeStamp", Instant.now().toEpochMilli())
                .put("status", "REPORTED");

        doAnswer(invocation -> {
            Message<JsonObject> msg = invocation.getArgument(0);
            msg.replyAndForget(new JsonObject().put("incident", incident));
            return null;
        }).when(eventBusConsumer).consume(any(Message.class));

        String body = given().when().get("/incidents/incident/incident1")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();
        JsonObject response = new JsonObject(body);
        assertThat(response, equalTo(incident));

        verify(eventBusConsumer).consume(messageCaptor.capture());
        assertThat(messageCaptor.getValue().headers().get("action"), equalTo("incidentById"));
        JsonObject payload = messageCaptor.getValue().body();
        assertThat(payload, notNullValue());
        assertThat(payload.getString("incidentId"), equalTo("incident1"));
    }

    @Test
    void testIncidentByIdNotFound() {

        doAnswer(invocation -> {
            Message<JsonObject> msg = invocation.getArgument(0);
            msg.replyAndForget(new JsonObject());
            return null;
        }).when(eventBusConsumer).consume(any(Message.class));

        given().when().get("/incidents/incident/incident1")
                .then().assertThat().statusCode(404).body(equalTo(""));

        verify(eventBusConsumer).consume(messageCaptor.capture());
        assertThat(messageCaptor.getValue().headers().get("action"), equalTo("incidentById"));
        JsonObject payload = messageCaptor.getValue().body();
        assertThat(payload, notNullValue());
        assertThat(payload.getString("incidentId"), equalTo("incident1"));
    }

    @Test
    void testIncidents() {

        JsonObject incident = new JsonObject().put("id", "incident1")
                .put("lat", "30.12345")
                .put("lon", "-70.98765")
                .put("medicalNeeded", true)
                .put("numberOfPeople", 3)
                .put("victimName", "John Doe")
                .put("victimPhoneNumber", "(123) 456-7890)")
                .put("timeStamp", Instant.now().toEpochMilli())
                .put("status", "REPORTED");

        doAnswer(invocation -> {
            Message<JsonObject> msg = invocation.getArgument(0);
            msg.replyAndForget(new JsonObject().put("incidents", new JsonArray().add(incident)));
            return null;
        }).when(eventBusConsumer).consume(any(Message.class));

        String body = given().when().get("/incidents")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();
        JsonArray response = new JsonArray(body);
        assertThat(response.size(), equalTo(1));
        assertThat(response.getJsonObject(0), equalTo(incident));

        verify(eventBusConsumer).consume(messageCaptor.capture());
        assertThat(messageCaptor.getValue().headers().get("action"), equalTo("incidents"));
        JsonObject payload = messageCaptor.getValue().body();
        assertThat(payload, notNullValue());
        assertThat(payload.isEmpty(), equalTo(true));
    }

    @Test
    void testIncidentsEmpty() {

        doAnswer(invocation -> {
            Message<JsonObject> msg = invocation.getArgument(0);
            msg.replyAndForget(new JsonObject().put("incidents", new JsonArray()));
            return null;
        }).when(eventBusConsumer).consume(any(Message.class));

        String body = given().when().get("/incidents")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();
        JsonArray response = new JsonArray(body);
        assertThat(response.size(), equalTo(0));

        verify(eventBusConsumer).consume(messageCaptor.capture());
        assertThat(messageCaptor.getValue().headers().get("action"), equalTo("incidents"));
        JsonObject payload = messageCaptor.getValue().body();
        assertThat(payload, notNullValue());
        assertThat(payload.isEmpty(), equalTo(true));
    }

    @Test
    void testIncidentsByStatus() {

        JsonObject incident = new JsonObject().put("id", "incident1")
                .put("lat", "30.12345")
                .put("lon", "-70.98765")
                .put("medicalNeeded", true)
                .put("numberOfPeople", 3)
                .put("victimName", "John Doe")
                .put("victimPhoneNumber", "(123) 456-7890)")
                .put("timeStamp", Instant.now().toEpochMilli())
                .put("status", "REPORTED");

        doAnswer(invocation -> {
            Message<JsonObject> msg = invocation.getArgument(0);
            msg.replyAndForget(new JsonObject().put("incidents", new JsonArray().add(incident)));
            return null;
        }).when(eventBusConsumer).consume(any(Message.class));

        String body = given().when().get("/incidents/reported")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();
        JsonArray response = new JsonArray(body);
        assertThat(response.size(), equalTo(1));
        assertThat(response.getJsonObject(0), equalTo(incident));

        verify(eventBusConsumer).consume(messageCaptor.capture());
        assertThat(messageCaptor.getValue().headers().get("action"), equalTo("incidentsByStatus"));
        JsonObject payload = messageCaptor.getValue().body();
        assertThat(payload, notNullValue());
        assertThat(payload.getString("status"), equalTo("reported"));
    }

    @Test
    void testIncidentsByStatusEmpty() {

        doAnswer(invocation -> {
            Message<JsonObject> msg = invocation.getArgument(0);
            msg.replyAndForget(new JsonObject().put("incidents", new JsonArray()));
            return null;
        }).when(eventBusConsumer).consume(any(Message.class));

        String body = given().when().get("/incidents/reported")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();
        JsonArray response = new JsonArray(body);
        assertThat(response.size(), equalTo(0));

        verify(eventBusConsumer).consume(messageCaptor.capture());
        assertThat(messageCaptor.getValue().headers().get("action"), equalTo("incidentsByStatus"));
        JsonObject payload = messageCaptor.getValue().body();
        assertThat(payload, notNullValue());
        assertThat(payload.getString("status"), equalTo("reported"));
    }

    @Test
    void testIncidentsByName() {

        JsonObject incident = new JsonObject().put("id", "incident1")
                .put("lat", "30.12345")
                .put("lon", "-70.98765")
                .put("medicalNeeded", true)
                .put("numberOfPeople", 3)
                .put("victimName", "John Doe")
                .put("victimPhoneNumber", "(123) 456-7890)")
                .put("timeStamp", Instant.now().toEpochMilli())
                .put("status", "REPORTED");

        doAnswer(invocation -> {
            Message<JsonObject> msg = invocation.getArgument(0);
            msg.replyAndForget(new JsonObject().put("incidents", new JsonArray().add(incident)));
            return null;
        }).when(eventBusConsumer).consume(any(Message.class));

        String body = given().when().get("/incidents/byname/John%")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();
        JsonArray response = new JsonArray(body);
        assertThat(response.size(), equalTo(1));
        assertThat(response.getJsonObject(0), equalTo(incident));

        verify(eventBusConsumer).consume(messageCaptor.capture());
        assertThat(messageCaptor.getValue().headers().get("action"), equalTo("incidentsByName"));
        JsonObject payload = messageCaptor.getValue().body();
        assertThat(payload, notNullValue());
        assertThat(payload.getString("name"), equalTo("John%"));
    }

    @Test
    void testIncidentsByNameEmpty() {

        doAnswer(invocation -> {
            Message<JsonObject> msg = invocation.getArgument(0);
            msg.replyAndForget(new JsonObject().put("incidents", new JsonArray()));
            return null;
        }).when(eventBusConsumer).consume(any(Message.class));

        String body = given().when().get("/incidents/byname/John%")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();
        JsonArray response = new JsonArray(body);
        assertThat(response.size(), equalTo(0));

        verify(eventBusConsumer).consume(messageCaptor.capture());
        assertThat(messageCaptor.getValue().headers().get("action"), equalTo("incidentsByName"));
        JsonObject payload = messageCaptor.getValue().body();
        assertThat(payload, notNullValue());
        assertThat(payload.getString("name"), equalTo("John%"));
    }

    @Test
    void testCreateIncident() {

        String body = "{\"lat\":30.12345,\"lon\":-70.98765,\"medicalNeeded\":true,"
                + "\"numberOfPeople\":3,\"victimName\":\"John Doe\",\"victimPhoneNumber\":\"(123) 456-7890)\"}";

        doAnswer(invocation -> {
            Message<JsonObject> msg = invocation.getArgument(0);
            msg.replyAndForget(new JsonObject());
            return null;
        }).when(eventBusConsumer).consume(any(Message.class));

        given().when().with().body(body).header(new Header("Content-Type", "application/json"))
                .post("/incidents")
                .then().assertThat().statusCode(200).body(CoreMatchers.equalTo(""));

        verify(eventBusConsumer).consume(messageCaptor.capture());
        assertThat(messageCaptor.getValue().headers().get("action"), equalTo("createIncident"));
        JsonObject payload = messageCaptor.getValue().body();
        assertThat(payload, equalTo(new JsonObject(body)));
    }

}
