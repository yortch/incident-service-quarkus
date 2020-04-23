package com.redhat.cajun.navy.incident.consumer;

import java.io.StringReader;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbConfig;

import com.redhat.cajun.navy.incident.message.IncidentEvent;
import com.redhat.cajun.navy.incident.message.Message;
import com.redhat.cajun.navy.incident.message.UpdateIncidentCommand;
import com.redhat.cajun.navy.incident.message.UpdateIncidentCommandMessageAdapter;
import com.redhat.cajun.navy.incident.model.Incident;
import com.redhat.cajun.navy.incident.service.IncidentService;
import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.axle.core.Promise;
import io.vertx.axle.core.Vertx;
import io.vertx.core.Handler;
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

    private FlowableProcessor<Incident> processor = UnicastProcessor.<Incident>create().toSerialized();

    @Inject
    IncidentService incidentService;

    @Inject
    Vertx vertx;

    @Incoming("incident-command")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<IncomingKafkaRecord<String, String>> processMessage(IncomingKafkaRecord<String, String> message) {
        try {
            acceptMessageType(message.getPayload()).ifPresent(m -> processUpdateIncidentCommand(message.getPayload()));
        } catch (Exception e) {
            log.error("Error processing msg " + message.getPayload(), e);
        }
        return message.ack().toCompletableFuture().thenApply(x -> message);
    }

    @SuppressWarnings("unchecked")
    private void processUpdateIncidentCommand(String messageAsJson) {

        Message<UpdateIncidentCommand> message;

        JsonbConfig config = new JsonbConfig().withAdapters(new UpdateIncidentCommandMessageAdapter());
        Jsonb jsonb = JsonbBuilder.newBuilder().withConfig(config).build();
        message = jsonb.fromJson(messageAsJson, Message.class);
        Incident incident = message.getBody().getIncident();

        log.debug("Processing '" + UPDATE_INCIDENT_COMMAND + "' message for incident '" + incident.getId() + "'");
        vertx.executeBlocking((Handler<Promise<Void>>) event -> {
            Incident updated = incidentService.updateIncident(incident);
            event.complete();
            processor.onNext(updated);
        });
    }

    private Optional<String> acceptMessageType(String messageAsJson) {
        try {
            JsonObject jsonReader = Json.createReader(new StringReader(messageAsJson)).readObject();
            String messageType = jsonReader.getString("messageType");
            if (Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(messageType)) {
                return Optional.of(messageType);
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

    private CompletionStage<org.eclipse.microprofile.reactive.messaging.Message<String>> toMessage(Incident incident) {
        Message<IncidentEvent> message = new Message.Builder<>("IncidentUpdatedEvent", "IncidentService",
                new IncidentEvent.Builder(incident.getId())
                        .lat(new BigDecimal(incident.getLat()))
                        .lon(new BigDecimal(incident.getLon()))
                        .medicalNeeded(incident.isMedicalNeeded())
                        .numberOfPeople(incident.getNumberOfPeople())
                        .timestamp(incident.getTimestamp())
                        .victimName(incident.getVictimName())
                        .victimPhoneNumber(incident.getVictimPhoneNumber())
                        .status(incident.getStatus())
                        .build())
                .build();
        Jsonb jsonb = JsonbBuilder.create();
        String json = jsonb.toJson(message);
        log.debug("Message: " + json);
        CompletableFuture<org.eclipse.microprofile.reactive.messaging.Message<String>> future = new CompletableFuture<>();
        KafkaRecord<String, String> kafkaMessage = KafkaRecord.of(incident.getId(), json);
        future.complete(kafkaMessage);
        return future;
    }

}
