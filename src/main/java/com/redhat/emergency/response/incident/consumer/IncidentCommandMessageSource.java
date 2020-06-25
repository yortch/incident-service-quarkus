package com.redhat.emergency.response.incident.consumer;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import com.redhat.emergency.response.incident.message.IncidentEvent;
import com.redhat.emergency.response.incident.message.Message;
import com.redhat.emergency.response.incident.service.IncidentService;
import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class IncidentCommandMessageSource {

    private final static Logger log = LoggerFactory.getLogger(IncidentCommandMessageSource.class);

    private static final String UPDATE_INCIDENT_COMMAND = "UpdateIncidentCommand";
    private static final String[] ACCEPTED_MESSAGE_TYPES = {UPDATE_INCIDENT_COMMAND};

    private FlowableProcessor<JsonObject> processor = UnicastProcessor.<JsonObject>create().toSerialized();

    @Inject
    IncidentService incidentService;

    @Inject
    Vertx vertx;

    @Incoming("incident-command")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<CompletionStage<Void>> processMessage(IncomingKafkaRecord<String, String> message) {

        return CompletableFuture.supplyAsync(() -> {
            try {
                acceptMessageType(message.getPayload()).ifPresent(this::processUpdateIncidentCommand);
            } catch (Exception e) {
                log.error("Error processing msg " + message.getPayload(), e);
            }
            return message.ack();
        });
    }

    @SuppressWarnings("unchecked")
    private void processUpdateIncidentCommand(JsonObject json) {

        JsonObject body = json.getJsonObject("body").getJsonObject("incident");
        log.debug("Processing '" + UPDATE_INCIDENT_COMMAND + "' message for incident '" + body.getString("id") + "'");
        JsonObject updated = incidentService.updateIncident(body);
        processor.onNext(updated);
    }

    private Optional<JsonObject> acceptMessageType(String messageAsJson) {
        try {
            JsonObject json = new JsonObject(messageAsJson);
            String messageType = json.getString("messageType");
            if (Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(messageType)) {
                if (json.containsKey("body") && json.getJsonObject("body").containsKey("incident")) {
                    return Optional.of(json);
                }
            }
            log.debug("Message with type '" + messageType + "' is ignored");
        } catch (Exception e) {
            log.warn("Unexpected message which is not JSON or without 'messageType' field.");
            log.warn("Message: " + messageAsJson);
        }
        return Optional.empty();
    }

    @Outgoing("incident-event")
    public PublisherBuilder<org.eclipse.microprofile.reactive.messaging.Message<String>> source() {
        return ReactiveStreams.fromPublisher(processor).flatMapCompletionStage(this::toMessage);
    }

    private CompletionStage<org.eclipse.microprofile.reactive.messaging.Message<String>> toMessage(JsonObject incident) {
        Message<IncidentEvent> message = new Message.Builder<>("IncidentUpdatedEvent", "IncidentService",
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
        Jsonb jsonb = null;
        jsonb = JsonbBuilder.create();
        String json = jsonb.toJson(message);
        log.debug("Message: " + json);
        CompletableFuture<org.eclipse.microprofile.reactive.messaging.Message<String>> future = new CompletableFuture<>();
        KafkaRecord<String, String> kafkaMessage = KafkaRecord.of(incident.getString("id"), json);
        future.complete(kafkaMessage);
        return future;
    }

}
