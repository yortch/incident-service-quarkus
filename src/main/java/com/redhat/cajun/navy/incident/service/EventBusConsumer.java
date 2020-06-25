package com.redhat.cajun.navy.incident.service;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import com.redhat.cajun.navy.incident.message.IncidentEvent;
import io.quarkus.vertx.ConsumeEvent;
import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class EventBusConsumer {

    private static Logger log = LoggerFactory.getLogger(EventBusConsumer.class);

    @Inject
    IncidentService service;

    private FlowableProcessor<JsonObject> processor = UnicastProcessor.<JsonObject>create().toSerialized();

    @ConsumeEvent(value = "incident-service", blocking = true)
    public void consume(Message<JsonObject> msg) {
        String action = msg.headers().get("action");
        switch (action) {
            case "incidents" :
                incidents(msg);
                break;
            case "incidentById" :
                incidentById(msg);
                break;
            case "incidentsByStatus":
                incidentsByStatus(msg);
                break;
            case "incidentsByName":
                incidentsByName(msg);
                break;
            case "reset" :
                reset(msg);
                break;
            case "createIncident":
                createIncident(msg);
                break;
            default:
                msg.fail(-1, "Unsupported operation");
        }
    }

    private void incidents(Message<JsonObject> msg) {
        JsonObject jsonObject = new JsonObject().put("incidents", service.incidents());
        msg.replyAndForget(jsonObject);
    }

    private void incidentById(Message<JsonObject> msg) {
        String id = msg.body().getString("incidentId");
        JsonObject incident = service.incidentByIncidentId(id);
        if (incident == null) {
            msg.replyAndForget(new JsonObject());
        } else {
            msg.replyAndForget(new JsonObject().put("incident", incident));
        }
    }

    private void incidentsByStatus(Message<JsonObject> msg) {
        String status = msg.body().getString("status");
        JsonArray incidentsArray = service.incidentsByStatus(status);
        JsonObject jsonObject = new JsonObject().put("incidents", incidentsArray);
        msg.replyAndForget(jsonObject);
    }

    private void incidentsByName(Message<JsonObject> msg) {
        String name = msg.body().getString("name");
        JsonArray incidentsArray = service.incidentsByVictimName(name);
        JsonObject jsonObject = new JsonObject().put("incidents", incidentsArray);
        msg.replyAndForget(jsonObject);
    }

    private void reset(Message<JsonObject> msg) {
        service.reset();
        msg.replyAndForget(new JsonObject());
    }

    private void createIncident(Message<JsonObject> msg) {
        JsonObject created = service.create(msg.body());
        processor.onNext(created);
        msg.replyAndForget(new JsonObject());
    }

    @Outgoing("incident-event")
    public PublisherBuilder<org.eclipse.microprofile.reactive.messaging.Message<String>> source() {
        return ReactiveStreams.fromPublisher(processor).flatMapCompletionStage(this::toMessage);
    }

    private CompletionStage<org.eclipse.microprofile.reactive.messaging.Message<String>> toMessage(JsonObject incident) {
        com.redhat.cajun.navy.incident.message.Message<IncidentEvent> message
                = new com.redhat.cajun.navy.incident.message.Message.Builder<>("IncidentReportedEvent", "IncidentService",
                    new IncidentEvent.Builder(incident.getString("id"))
                        .lat(new BigDecimal(incident.getString("lat")))
                        .lon(new BigDecimal(incident.getString("lon")))
                        .medicalNeeded(incident.getBoolean("medicalNeeded"))
                        .numberOfPeople(incident.getInteger("numberOfPeople"))
                        .timestamp(incident.getLong("timestamp"))
                        .victimName(incident.getString("victimName"))
                        .victimPhoneNumber(incident.getString("victimPhoneNumber"))
                        .status(incident.getString("status"))
                        .build())
                .build();
        Jsonb jsonb = JsonbBuilder.create();
        String json = jsonb.toJson(message);
        log.debug("Message: " + json);
        CompletableFuture<org.eclipse.microprofile.reactive.messaging.Message<String>> future = new CompletableFuture<>();
        KafkaRecord<String, String> kafkaMessage = KafkaRecord.of(incident.getString("id"), json);
        future.complete(kafkaMessage);
        return future;

    }
}
