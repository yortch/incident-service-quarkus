package com.redhat.cajun.navy.incident.service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;

import com.redhat.cajun.navy.incident.dao.IncidentDao;
import com.redhat.cajun.navy.incident.model.Incident;
import com.redhat.cajun.navy.incident.model.IncidentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class IncidentService {

    private static final Logger log = LoggerFactory.getLogger(IncidentService.class);

    @Inject
    IncidentDao incidentDao;

    @Transactional
    public List<Incident> incidents() {
        return incidentDao.findAll().stream().map(this::fromEntity).collect(Collectors.toList());
    }

    @Transactional
    public Incident create(Incident incident) {
        com.redhat.cajun.navy.incident.entity.Incident created = incidentDao.create(toEntity(incident));

        return fromEntity(created);
    }

    @Transactional
    public Incident updateIncident(Incident incident) {
        com.redhat.cajun.navy.incident.entity.Incident current = incidentDao.findByIncidentId(incident.getId());
        if (current == null) {
            log.warn("Incident with id '" + incident.getId() + "' not found in the database");
            return null;
        }
        if (incident.getLat() != null && !incident.getLat().equals(current.getLatitude())) {
            current.setLatitude(incident.getLat());
        }
        if (incident.getLon() != null && !incident.getLon().equals(current.getLatitude())) {
            current.setLongitude(incident.getLon());
        }
        if (incident.getNumberOfPeople() != null && !incident.getNumberOfPeople().equals(current.getNumberOfPeople())) {
            current.setNumberOfPeople(incident.getNumberOfPeople());
        }
        if (incident.isMedicalNeeded() != null && !incident.isMedicalNeeded().equals(current.isMedicalNeeded())) {
            current.setMedicalNeeded(incident.isMedicalNeeded());
        }
        if (incident.getVictimName() != null && !incident.getVictimName().equals(current.getVictimName())) {
            current.setVictimName(incident.getVictimName());
        }
        if (incident.getVictimPhoneNumber() != null && !incident.getVictimPhoneNumber().equals(current.getVictimPhoneNumber())) {
            current.setVictimPhoneNumber(incident.getVictimPhoneNumber());
        }
        if (incident.getStatus() != null && !incident.getStatus().equals(current.getStatus())) {
            current.setStatus(incident.getStatus());
        }
        return fromEntity(current);
    }

    @Transactional
    public Incident incidentByIncidentId(String incidentId) {
        return fromEntity(incidentDao.findByIncidentId(incidentId));
    }

    @Transactional
    public List<Incident> incidentsByStatus(String status) {
        return incidentDao.findByStatus(status).stream().map(this::fromEntity).collect(Collectors.toList());
    }

    @Transactional
    public List<Incident> incidentsByVictimName(String name) {
        return incidentDao.findByName(name).stream().map(this::fromEntity).collect(Collectors.toList());
    }

    @Transactional
    public void reset() {
        incidentDao.deleteAll();
    }

    private Incident fromEntity(com.redhat.cajun.navy.incident.entity.Incident r) {

        if (r == null) {
            return null;
        }
        return new Incident.Builder(r.getIncidentId())
                .lat(r.getLatitude())
                .lon(r.getLongitude())
                .medicalNeeded(r.isMedicalNeeded())
                .numberOfPeople(r.getNumberOfPeople())
                .victimName(r.getVictimName())
                .victimPhoneNumber(r.getVictimPhoneNumber())
                .status(r.getStatus())
                .timestamp(r.getTimestamp())
                .build();
    }

    private com.redhat.cajun.navy.incident.entity.Incident toEntity(Incident incident) {

        String incidentId = UUID.randomUUID().toString();
        long reportedTimestamp = System.currentTimeMillis();

        com.redhat.cajun.navy.incident.entity.Incident entity = new com.redhat.cajun.navy.incident.entity.Incident();
        entity.setIncidentId(incidentId);
        entity.setLatitude(incident.getLat());
        entity.setLongitude(incident.getLon());
        entity.setMedicalNeeded(incident.isMedicalNeeded());
        entity.setNumberOfPeople(incident.getNumberOfPeople());
        entity.setVictimName(incident.getVictimName());
        entity.setVictimPhoneNumber(incident.getVictimPhoneNumber());
        entity.setReportedTime(Instant.ofEpochMilli(reportedTimestamp));
        entity.setStatus(IncidentStatus.REPORTED.name());
        return entity;
    }

}
