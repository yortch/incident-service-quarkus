package com.redhat.emergency.response.incident.rest;

import java.util.concurrent.CompletionStage;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.EventBus;

@Path("/incidents")
public class IncidentsResource {

    @Inject
    EventBus bus;

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public CompletionStage<Response> incidents() {
        DeliveryOptions options = new DeliveryOptions().addHeader("action", "incidents");
        return bus.<JsonObject>request("incident-service", new JsonObject(), options)
                .subscribeAsCompletionStage()
                .thenApply(msg -> Response.ok(msg.body().getJsonArray("incidents").encode()).build());
    }

    @POST
    @Path("/")
    @Consumes(MediaType.APPLICATION_JSON)
    public CompletionStage<Response> createIncident(String incident) {
        DeliveryOptions options = new DeliveryOptions().addHeader("action", "createIncident");
        return bus.<JsonObject>request("incident-service", new JsonObject(incident), options)
                .subscribeAsCompletionStage()
                .thenApply(msg -> Response.status(200).build());
    }

    @GET
    @Path("/{status}")
    @Produces(MediaType.APPLICATION_JSON)
    public CompletionStage<Response> incidentsByStatus(@PathParam("status") String status) {
        DeliveryOptions options = new DeliveryOptions().addHeader("action", "incidentsByStatus");
        return bus.<JsonObject>request("incident-service", new JsonObject().put("status", status), options)
                .subscribeAsCompletionStage()
                .thenApply(msg -> Response.ok(msg.body().getJsonArray("incidents").encode()).build());
    }

    @GET
    @Path("/incident/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public CompletionStage<Response> incidentById(@PathParam("id") String incidentId) {
        DeliveryOptions options = new DeliveryOptions().addHeader("action", "incidentById");
        return bus.<JsonObject>request("incident-service",  new JsonObject().put("incidentId", incidentId), options)
                .subscribeAsCompletionStage()
                .thenApply(msg -> {
                    JsonObject incident = msg.body().getJsonObject("incident");
                    if (incident == null) {
                        return Response.status(404).build();
                    } else {
                        return Response.ok(incident.encode()).build();
                    }
                });
    }

    @GET
    @Path("/byname/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public CompletionStage<Response> incidentsByName(@PathParam("name") String name) {
        DeliveryOptions options = new DeliveryOptions().addHeader("action", "incidentsByName");
        return bus.<JsonObject>request("incident-service", new JsonObject().put("name", name), options)
                .subscribeAsCompletionStage()
                .thenApply(msg -> Response.ok(msg.body().getJsonArray("incidents").encode()).build());
    }

    @POST
    @Path("/reset")
    public CompletionStage<Response> reset() {
        DeliveryOptions options = new DeliveryOptions().addHeader("action", "reset");
        return bus.<JsonObject>request("incident-service", new JsonObject(), options)
                .subscribeAsCompletionStage()
                .thenApply(msg -> Response.ok().build());
    }

}
