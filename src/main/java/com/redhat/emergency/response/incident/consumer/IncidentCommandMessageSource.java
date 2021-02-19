package com.redhat.emergency.response.incident.consumer;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.emergency.response.incident.service.IncidentService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.ce.IncomingCloudEventMetadata;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class IncidentCommandMessageSource {

    private final static Logger log = LoggerFactory.getLogger(IncidentCommandMessageSource.class);

    private static final String UPDATE_INCIDENT_COMMAND = "UpdateIncidentCommand";
    private static final String[] ACCEPTED_MESSAGE_TYPES = {UPDATE_INCIDENT_COMMAND};

    private final UnicastProcessor<JsonObject> processor = UnicastProcessor.create();

    @Inject
    IncidentService incidentService;

    @Incoming("incident-command")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<CompletionStage<Void>> processMessage(IncomingKafkaRecord<String, String> message) {

        return CompletableFuture.supplyAsync(() -> {
            try {
                acceptMessageType(message).ifPresent(this::processUpdateIncidentCommand);
            } catch (Exception e) {
                log.error("Error processing msg " + message.getPayload(), e);
            }
            return message.ack();
        });
    }

    private void processUpdateIncidentCommand(JsonObject json) {

        JsonObject incident = json.getJsonObject("incident");
        log.debug("Processing '" + UPDATE_INCIDENT_COMMAND + "' message for incident '" + incident.getString("id") + "'");
        JsonObject updated = incidentService.updateIncident(incident);
        processor.onNext(updated);
    }

    private Optional<JsonObject> acceptMessageType(IncomingKafkaRecord<String, String> message) {
        try {
            Optional<IncomingCloudEventMetadata> metadata = message.getMetadata(IncomingCloudEventMetadata.class);
            if (metadata.isEmpty()) {
                log.warn("Incoming message is not a CloudEvent");
                return Optional.empty();
            }
            IncomingCloudEventMetadata<String> cloudEventMetadata = metadata.get();
            String dataContentType = cloudEventMetadata.getDataContentType().orElse("");
            if (!dataContentType.equalsIgnoreCase("application/json")) {
                log.warn("CloudEvent data content type is not specified or not 'application/json'. Message is ignored");
                return Optional.empty();
            }
            String type = cloudEventMetadata.getType();
            if (!(Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(type))) {
                log.debug("CloudEvent with type '" + type + "' is ignored");
                return Optional.empty();
            }
            JsonObject json = new JsonObject(message.getPayload());
            if (json.containsKey("incident")) {
                return Optional.of(json);
            } else {
                log.warn("Message payload does not contain incident: " + message.getPayload());
                return Optional.empty();
            }
        } catch (Exception e) {
            log.warn("Unexpected message is ignored: " + message.getPayload());
            return Optional.empty();
        }

    }

    @Outgoing("incident-event")
    public Multi<Message<String>> source() {
        return processor.onItem().transform(this::toMessage);
    }

    private Message<String> toMessage(JsonObject incident) {
        log.debug("IncidentUpdatedEvent: " + incident.toString());
        return KafkaRecord.of(incident.getString("id"), incident.toString())
                .addMetadata(OutgoingCloudEventMetadata.builder().withType("IncidentUpdatedEvent")
                        .withTimestamp(OffsetDateTime.now().toZonedDateTime()).build());
    }
}
