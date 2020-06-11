package com.redhat.cajun.navy.incident.rest;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.eclipse.microprofile.health.Readiness;

@Liveness
@Readiness
@ApplicationScoped
public class ApplicationHealthCheck implements HealthCheck {


    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("Health check").up().build();
    }
}
