package com.redhat.cajun.navy.incident.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;

import com.redhat.cajun.navy.incident.dao.IncidentDao;
import com.redhat.cajun.navy.incident.entity.Incident;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;

@QuarkusTest
public class IncidentServiceTest {

    @InjectMock
    IncidentDao incidentDao;

    @Inject
    IncidentService incidentService;

    @Captor
    ArgumentCaptor<Incident> incidentCaptor;

    @BeforeEach
    void init() {
        initMocks(this);
    }

    @Test
    void testIncidents() {

        Incident incident1 = new Incident();
        incident1.setIncidentId("incident1");
        incident1.setLatitude("30.12345");
        incident1.setLongitude("-70.98765");
        incident1.setNumberOfPeople(3);
        incident1.setMedicalNeeded(true);
        incident1.setVictimName("John Doe I");
        incident1.setVictimPhoneNumber("(111) 456-78990");
        incident1.setReportedTime(Instant.now());
        incident1.setStatus("REPORTED");

        Incident incident2 = new Incident();
        incident2.setIncidentId("incident2");
        incident2.setLatitude("31.12345");
        incident2.setLongitude("-71.98765");
        incident2.setNumberOfPeople(4);
        incident2.setMedicalNeeded(true);
        incident2.setVictimName("John Doe II");
        incident2.setVictimPhoneNumber("(211) 456-78990");
        incident2.setReportedTime(Instant.now());
        incident2.setStatus("ASSIGNED");

        Incident incident3 = new Incident();
        incident3.setIncidentId("incident3");
        incident3.setLatitude("32.12345");
        incident3.setLongitude("-72.98765");
        incident3.setNumberOfPeople(5);
        incident3.setMedicalNeeded(true);
        incident3.setVictimName("John Doe III");
        incident3.setVictimPhoneNumber("(311) 456-78990");
        incident3.setReportedTime(Instant.now());
        incident3.setStatus("RESCUED");

        when(incidentDao.findAll()).thenReturn(Arrays.asList(incident1, incident2, incident3));

        List<com.redhat.cajun.navy.incident.model.Incident> incidents = incidentService.incidents();

        assertThat(incidents, notNullValue());
        assertThat(incidents.size(), equalTo(3));
        assertThat(incidents.get(0).getId(), anyOf(equalTo("incident1"), equalTo("incident2"),equalTo("incident3")));
        com.redhat.cajun.navy.incident.model.Incident matched = incidents.stream().filter(i -> i.getId().equals("incident2")).findFirst().orElse(null);
        assertThat(matched, notNullValue());
        assertThat(matched.getLat(), equalTo(incident2.getLatitude()));
        assertThat(matched.getLon(), equalTo(incident2.getLongitude()));
        assertThat(matched.isMedicalNeeded(), equalTo(incident2.isMedicalNeeded()));
        assertThat(matched.getNumberOfPeople(), equalTo(incident2.getNumberOfPeople()));
        assertThat(matched.getVictimName(), equalTo(incident2.getVictimName()));
        assertThat(matched.getVictimPhoneNumber(), equalTo(incident2.getVictimPhoneNumber()));
        assertThat(matched.getTimestamp(), equalTo(incident2.getTimestamp()));
        assertThat(matched.getStatus(), equalTo(incident2.getStatus()));
    }

    @Test
    void testCreate() {

        Incident incidentEntity = new Incident();
        incidentEntity.setIncidentId("incident2");
        incidentEntity.setLatitude("31.12345");
        incidentEntity.setLongitude("-71.98765");
        incidentEntity.setNumberOfPeople(4);
        incidentEntity.setMedicalNeeded(true);
        incidentEntity.setVictimName("John Doe");
        incidentEntity.setVictimPhoneNumber("(211) 456-78990");
        incidentEntity.setReportedTime(Instant.now());
        incidentEntity.setStatus("REPORTED");

        when(incidentDao.create(Mockito.any(Incident.class))).thenReturn(incidentEntity);

        com.redhat.cajun.navy.incident.model.Incident incident = new com.redhat.cajun.navy.incident.model.Incident.Builder()
                .lat("31.12345")
                .lon("-71.98765")
                .numberOfPeople(4)
                .medicalNeeded(true)
                .victimName("John Doe")
                .victimPhoneNumber("(211) 456-78990")
                .build();

        com.redhat.cajun.navy.incident.model.Incident created = incidentService.create(incident);

        assertThat(created, notNullValue());
        assertThat(created.getId(), equalTo(incidentEntity.getIncidentId()));
        assertThat(created.getLat(), equalTo(incidentEntity.getLatitude()));
        assertThat(created.getLon(), equalTo(incidentEntity.getLongitude()));
        assertThat(created.isMedicalNeeded(), equalTo(incidentEntity.isMedicalNeeded()));
        assertThat(created.getNumberOfPeople(), equalTo(incidentEntity.getNumberOfPeople()));
        assertThat(created.getVictimName(), equalTo(incidentEntity.getVictimName()));
        assertThat(created.getVictimPhoneNumber(), equalTo(incidentEntity.getVictimPhoneNumber()));
        assertThat(created.getTimestamp(), equalTo(incidentEntity.getTimestamp()));
        assertThat(created.getStatus(), equalTo(incidentEntity.getStatus()));

        verify(incidentDao).create(incidentCaptor.capture());
        Incident captured = incidentCaptor.getValue();
        assertThat(captured, notNullValue());
        assertThat(captured.getIncidentId().length(), equalTo(36));
        assertThat(captured.getStatus(), equalTo("REPORTED"));
        assertThat(captured.getLatitude(), equalTo(incident.getLat()));
        assertThat(captured.getLongitude(), equalTo(incident.getLon()));
        assertThat(captured.getNumberOfPeople(), equalTo(incident.getNumberOfPeople()));
        assertThat(captured.isMedicalNeeded(), equalTo(incident.isMedicalNeeded()));
        assertThat(captured.getVictimName(), equalTo(incident.getVictimName()));
        assertThat(captured.getVictimPhoneNumber(), equalTo(incident.getVictimPhoneNumber()));
    }

    @Test
    void testUpdateIncident() {

        Incident incidentEntity = new Incident();
        incidentEntity.setIncidentId("incident2");
        incidentEntity.setLatitude("31.12345");
        incidentEntity.setLongitude("-71.98765");
        incidentEntity.setNumberOfPeople(4);
        incidentEntity.setMedicalNeeded(true);
        incidentEntity.setVictimName("John Doe");
        incidentEntity.setVictimPhoneNumber("(211) 456-78990");
        incidentEntity.setReportedTime(Instant.now());
        incidentEntity.setStatus("REPORTED");

        when(incidentDao.findByIncidentId("incident2")).thenReturn(incidentEntity);

        com.redhat.cajun.navy.incident.model.Incident incident = new com.redhat.cajun.navy.incident.model.Incident.Builder("incident2")
                .lat("32.12345")
                .lon("-72.98765")
                .status("ASSIGNED")
                .build();

        com.redhat.cajun.navy.incident.model.Incident updated = incidentService.updateIncident(incident);
        assertThat(updated, notNullValue());
        assertThat(updated.getId(), equalTo("incident2"));
        assertThat(updated.getLat(), equalTo(incident.getLat()));
        assertThat(updated.getLon(), equalTo(incident.getLon()));
        assertThat(updated.getStatus(), equalTo(incident.getStatus()));
        assertThat(updated.getNumberOfPeople(), equalTo(incidentEntity.getNumberOfPeople()));
        assertThat(updated.isMedicalNeeded(), equalTo(incidentEntity.isMedicalNeeded()));
        assertThat(updated.getVictimName(), equalTo(incidentEntity.getVictimName()));
        assertThat(updated.getVictimPhoneNumber(), equalTo(incidentEntity.getVictimPhoneNumber()));

        assertThat(incidentEntity.getLatitude(), equalTo(incident.getLat()));
        assertThat(incidentEntity.getLongitude(), equalTo(incident.getLon()));
        assertThat(incidentEntity.getStatus(), equalTo(incident.getStatus()));
        verify(incidentDao).findByIncidentId("incident2");
    }

    @Test
    void testIncidentById() {
        Incident incidentEntity = new Incident();
        incidentEntity.setIncidentId("incident2");
        incidentEntity.setLatitude("31.12345");
        incidentEntity.setLongitude("-71.98765");
        incidentEntity.setNumberOfPeople(4);
        incidentEntity.setMedicalNeeded(true);
        incidentEntity.setVictimName("John Doe");
        incidentEntity.setVictimPhoneNumber("(211) 456-78990");
        incidentEntity.setReportedTime(Instant.now());
        incidentEntity.setStatus("REPORTED");

        when(incidentDao.findByIncidentId("incident2")).thenReturn(incidentEntity);

        com.redhat.cajun.navy.incident.model.Incident found = incidentService.incidentByIncidentId("incident2");

        assertThat(found, notNullValue());
        assertThat(found.getId(), equalTo(incidentEntity.getIncidentId()));
        assertThat(found.getLat(), equalTo(incidentEntity.getLatitude()));
        assertThat(found.getLon(), equalTo(incidentEntity.getLongitude()));
        assertThat(found.isMedicalNeeded(), equalTo(incidentEntity.isMedicalNeeded()));
        assertThat(found.getNumberOfPeople(), equalTo(incidentEntity.getNumberOfPeople()));
        assertThat(found.getVictimName(), equalTo(incidentEntity.getVictimName()));
        assertThat(found.getVictimPhoneNumber(), equalTo(incidentEntity.getVictimPhoneNumber()));
        assertThat(found.getTimestamp(), equalTo(incidentEntity.getTimestamp()));
        assertThat(found.getStatus(), equalTo(incidentEntity.getStatus()));
        verify(incidentDao).findByIncidentId("incident2");
    }


    @Test
    void testIncidentByIdNotFound() {

        when(incidentDao.findByIncidentId("incident2")).thenReturn(null);

        com.redhat.cajun.navy.incident.model.Incident found = incidentService.incidentByIncidentId("incident2");

        assertThat(found, nullValue());
        verify(incidentDao).findByIncidentId("incident2");
    }

    @Test
    void testIncidentByStatus() {

        Incident incidentEntity = new Incident();
        incidentEntity.setIncidentId("incident1");
        incidentEntity.setLatitude("31.12345");
        incidentEntity.setLongitude("-71.98765");
        incidentEntity.setNumberOfPeople(4);
        incidentEntity.setMedicalNeeded(true);
        incidentEntity.setVictimName("John Doe");
        incidentEntity.setVictimPhoneNumber("(211) 456-78990");
        incidentEntity.setReportedTime(Instant.now());
        incidentEntity.setStatus("REPORTED");

        Incident incidentEntity2 = new Incident();
        incidentEntity2.setIncidentId("incident2");
        incidentEntity2.setLatitude("32.12345");
        incidentEntity2.setLongitude("-72.98765");
        incidentEntity2.setNumberOfPeople(5);
        incidentEntity2.setMedicalNeeded(false);
        incidentEntity2.setVictimName("John Foo");
        incidentEntity2.setVictimPhoneNumber("(311) 456-78990");
        incidentEntity2.setReportedTime(Instant.now());
        incidentEntity2.setStatus("REPORTED");

        when(incidentDao.findByStatus("REPORTED")).thenReturn(Arrays.asList(incidentEntity, incidentEntity2));

        List<com.redhat.cajun.navy.incident.model.Incident> incidents = incidentService.incidentsByStatus("REPORTED");

        assertThat(incidents, notNullValue());
        assertThat(incidents.size(), equalTo(2));
        assertThat(incidents.get(0).getId(), anyOf(equalTo("incident1"), equalTo("incident2")));
        assertThat(incidents.get(1).getId(), anyOf(equalTo("incident1"), equalTo("incident2")));
        assertThat(incidents.get(0).getId(), not(equalTo(incidents.get(1))));
        com.redhat.cajun.navy.incident.model.Incident found = incidents.stream().filter(i -> i.getId().equals("incident2")).findFirst().orElse(null);
        assertThat(found, notNullValue());
        assertThat(found.getLat(), equalTo(incidentEntity2.getLatitude()));
        assertThat(found.getLon(), equalTo(incidentEntity2.getLongitude()));
        assertThat(found.isMedicalNeeded(), equalTo(incidentEntity2.isMedicalNeeded()));
        assertThat(found.getNumberOfPeople(), equalTo(incidentEntity2.getNumberOfPeople()));
        assertThat(found.getVictimName(), equalTo(incidentEntity2.getVictimName()));
        assertThat(found.getVictimPhoneNumber(), equalTo(incidentEntity2.getVictimPhoneNumber()));
        assertThat(found.getTimestamp(), equalTo(incidentEntity2.getTimestamp()));
        assertThat(found.getStatus(), equalTo(incidentEntity2.getStatus()));

        verify(incidentDao).findByStatus("REPORTED");
    }

    @Test
    void testIncidentByStatusNotFound() {

        when(incidentDao.findByStatus("REPORTED")).thenReturn(Collections.emptyList());

        List<com.redhat.cajun.navy.incident.model.Incident> incidents = incidentService.incidentsByStatus("REPORTED");

        assertThat(incidents, notNullValue());
        assertThat(incidents.size(), equalTo(0));

        verify(incidentDao).findByStatus("REPORTED");
    }

    @Test
    void testIncidentByVictimName() {

        Incident incidentEntity = new Incident();
        incidentEntity.setIncidentId("incident1");
        incidentEntity.setLatitude("31.12345");
        incidentEntity.setLongitude("-71.98765");
        incidentEntity.setNumberOfPeople(4);
        incidentEntity.setMedicalNeeded(true);
        incidentEntity.setVictimName("John Doe");
        incidentEntity.setVictimPhoneNumber("(211) 456-78990");
        incidentEntity.setReportedTime(Instant.now());
        incidentEntity.setStatus("REPORTED");

        Incident incidentEntity2 = new Incident();
        incidentEntity2.setIncidentId("incident2");
        incidentEntity2.setLatitude("32.12345");
        incidentEntity2.setLongitude("-72.98765");
        incidentEntity2.setNumberOfPeople(5);
        incidentEntity2.setMedicalNeeded(false);
        incidentEntity2.setVictimName("John Foo");
        incidentEntity2.setVictimPhoneNumber("(311) 456-78990");
        incidentEntity2.setReportedTime(Instant.now());
        incidentEntity2.setStatus("REPORTED");

        when(incidentDao.findByName("John%")).thenReturn(Arrays.asList(incidentEntity, incidentEntity2));

        List<com.redhat.cajun.navy.incident.model.Incident> incidents = incidentService.incidentsByVictimName("John%");

        assertThat(incidents, notNullValue());
        assertThat(incidents.size(), equalTo(2));
        assertThat(incidents.get(0).getId(), anyOf(equalTo("incident1"), equalTo("incident2")));
        assertThat(incidents.get(1).getId(), anyOf(equalTo("incident1"), equalTo("incident2")));
        assertThat(incidents.get(0).getId(), not(equalTo(incidents.get(1))));
        com.redhat.cajun.navy.incident.model.Incident found = incidents.stream().filter(i -> i.getId().equals("incident2")).findFirst().orElse(null);
        assertThat(found, notNullValue());
        assertThat(found.getLat(), equalTo(incidentEntity2.getLatitude()));
        assertThat(found.getLon(), equalTo(incidentEntity2.getLongitude()));
        assertThat(found.isMedicalNeeded(), equalTo(incidentEntity2.isMedicalNeeded()));
        assertThat(found.getNumberOfPeople(), equalTo(incidentEntity2.getNumberOfPeople()));
        assertThat(found.getVictimName(), equalTo(incidentEntity2.getVictimName()));
        assertThat(found.getVictimPhoneNumber(), equalTo(incidentEntity2.getVictimPhoneNumber()));
        assertThat(found.getTimestamp(), equalTo(incidentEntity2.getTimestamp()));
        assertThat(found.getStatus(), equalTo(incidentEntity2.getStatus()));

        verify(incidentDao).findByName("John%");
    }

    @Test
    void testIncidentByVictimNameNotFound() {

        when(incidentDao.findByName("John%")).thenReturn(Collections.emptyList());

        List<com.redhat.cajun.navy.incident.model.Incident> incidents = incidentService.incidentsByVictimName("John%");

        assertThat(incidents, notNullValue());
        assertThat(incidents.size(), equalTo(0));
        verify(incidentDao).findByName("John%");
    }

    @Test
    void testReset() {

        incidentService.reset();

        verify(incidentDao).deleteAll();
    }



}
