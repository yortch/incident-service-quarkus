package com.redhat.emergency.response.incident.repository;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.transaction.Transactional;
import javax.transaction.UserTransaction;

import com.redhat.emergency.response.incident.entity.Incident;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class IncidentRepositoryTest {

    @Inject
    IncidentRepository incidentDao;

    @Inject
    EntityManager entityManager;

    @Inject
    UserTransaction transaction;

    @BeforeEach
    @Transactional
    void clearTable() {
        entityManager.createQuery("DELETE FROM Incident").executeUpdate();
    }

    /**
     *  Test description:
     *
     *    When:
     *      An Incident is created
     *
     *    Then:
     *      The id of the Incident entity is not equal to 0
     *
     */
    @Test
    @Transactional
    void testCreateIncident() {
        Incident incident = new Incident();
        incident.setIncidentId("qwertyuiop");
        incident.setLatitude("30.12345");
        incident.setLongitude("-70.98765");
        incident.setNumberOfPeople(3);
        incident.setMedicalNeeded(true);
        incident.setVictimName("John Doe");
        incident.setVictimPhoneNumber("(111) 456-78990");
        incident.setReportedTime(Instant.now());
        incident.setStatus("REPORTED");

        incidentDao.create(incident);
        assertThat(incident.getId(), not(equalTo(0)));
    }

    /**
     *  Test description:
     *
     *    When:
     *      There are Incident records in the database
     *      A call is made to `findAll`
     *
     *    Then:
     *      A list of Incident objects is returned
     *      The size of the list is equal to the number of incidents in the database
     *      Every Incident in the list is fully populated
     *
     */
    @Test
    void testFindAll() {
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

        createIncidents(Arrays.asList(incident1, incident2, incident3));

        List<Incident> incidents = new TransactionTemplate(transaction).execute(() -> incidentDao.findAll());
        assertThat(incidents.size(), equalTo(3));
        assertThat(incidents.get(0).getIncidentId(), anyOf(equalTo("incident1"), equalTo("incident2"), equalTo("incident3")));
        Incident matched = incidents.stream().filter(incident -> incident.getIncidentId().equals("incident1")).findFirst().orElse(null);
        assertThat(matched, notNullValue());
        assertThat(matched.getVictimName(), equalTo(incident1.getVictimName()));
        assertThat(matched.getVictimPhoneNumber(), equalTo(incident1.getVictimPhoneNumber()));
        assertThat(matched.getLatitude(), equalTo(incident1.getLatitude()));
        assertThat(matched.getLongitude(), equalTo(incident1.getLongitude()));
        assertThat(matched.getNumberOfPeople(), equalTo(incident1.getNumberOfPeople()));
        assertThat(matched.isMedicalNeeded(), equalTo(incident1.isMedicalNeeded()));
        assertThat(matched.getReportedTime(), equalTo(incident1.getReportedTime()));
        assertThat(matched.getStatus(), equalTo(incident1.getStatus()));
    }

    /**
     *  Test description:
     *
     *    When:
     *      There are no Incident records in the database
     *      A call is made to `findAll`
     *
     *    Then:
     *      An empty list is returned
     *
     */
    @Test
    @Transactional
    void testFindAllNoIncidents() {

        List<Incident> incidents = incidentDao.findAll();
        assertThat(incidents, notNullValue());
        assertThat(incidents.size(), equalTo(0));
    }

    /**
     *  Test description:
     *
     *    When:
     *      There are Incident records in the database
     *      There is an incident with the given incidentId in the database
     *      A call is made to `findById`
     *
     *    Then:
     *      An Incident entity is returned
     *      The entity is fully populated
     *
     */
    @Test
    void findByIncidentId() {
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

        createIncidents(Arrays.asList(incident1, incident2, incident3));

        Incident found = new TransactionTemplate(transaction).execute(() -> incidentDao.findByIncidentId("incident2"));
        assertThat(found, notNullValue());
        assertThat(found.getIncidentId(), equalTo(incident2.getIncidentId()));
        assertThat(found.getVictimName(), equalTo(incident2.getVictimName()));
        assertThat(found.getVictimPhoneNumber(), equalTo(incident2.getVictimPhoneNumber()));
        assertThat(found.getLatitude(), equalTo(incident2.getLatitude()));
        assertThat(found.getLongitude(), equalTo(incident2.getLongitude()));
        assertThat(found.getNumberOfPeople(), equalTo(incident2.getNumberOfPeople()));
        assertThat(found.isMedicalNeeded(), equalTo(incident2.isMedicalNeeded()));
        assertThat(found.getReportedTime(), equalTo(incident2.getReportedTime()));
        assertThat(found.getStatus(), equalTo(incident2.getStatus()));
    }

    /**
     *  Test description:
     *
     *    When:
     *      There are Incident records in the database
     *      There is no incident with the given incidentId in the database
     *      A call is made to `findById`
     *
     *    Then:
     *      The call returns a null value
     *
     */
    @Test
    void findByIncidentNotFound() {
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

        createIncidents(Arrays.asList(incident1, incident2, incident3));

        Incident found = new TransactionTemplate(transaction).execute(() -> incidentDao.findByIncidentId("incident4"));
        assertThat(found, nullValue());
    }

    /**
     *  Test description:
     *
     *    When:
     *      There are Incident records in the database
     *      There is an incident with the given status in the database
     *      A call is made to `findByStatus`
     *
     *    Then:
     *      A list of Incident entities is returned
     *      The entities are fully populated
     *
     */
    @Test
    void findByStatus() {
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

        createIncidents(Arrays.asList(incident1, incident2, incident3));

        List<Incident> incidents = new TransactionTemplate(transaction).execute(() ->incidentDao.findByStatus("ASSIGNED"));
        assertThat(incidents, notNullValue());
        assertThat(incidents.size(), equalTo(1));
        Incident found = incidents.get(0);
        assertThat(found.getIncidentId(), equalTo(incident2.getIncidentId()));
        assertThat(found.getVictimName(), equalTo(incident2.getVictimName()));
        assertThat(found.getVictimPhoneNumber(), equalTo(incident2.getVictimPhoneNumber()));
        assertThat(found.getLatitude(), equalTo(incident2.getLatitude()));
        assertThat(found.getLongitude(), equalTo(incident2.getLongitude()));
        assertThat(found.getNumberOfPeople(), equalTo(incident2.getNumberOfPeople()));
        assertThat(found.isMedicalNeeded(), equalTo(incident2.isMedicalNeeded()));
        assertThat(found.getReportedTime(), equalTo(incident2.getReportedTime()));
        assertThat(found.getStatus(), equalTo(incident2.getStatus()));
    }

    /**
     *  Test description:
     *
     *    When:
     *      There are Incident records in the database
     *      There is no Incident with the given status in the database
     *      A call is made to `findByStatus`
     *
     *    Then:
     *      A empty list is returned
     *
     */
    @Test
    void findByStatusNotFound() {
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

        createIncidents(Arrays.asList(incident1, incident2, incident3));

        List<Incident> incidents = new TransactionTemplate(transaction).execute(() ->incidentDao.findByStatus("UNKNOWN"));
        assertThat(incidents, notNullValue());
        assertThat(incidents.size(), equalTo(0));
    }

    /**
     *  Test description:
     *
     *    When:
     *      There are Incident records in the database
     *      There is an Incident with the name which exactly matches the given name
     *      A call is made to `findByName`
     *
     *    Then:
     *      A list of Incident objects is returned
     *      The list contains one element
     *      The name of the found Incidents matches exactly the given name
     *      Every Incident in the list is fully populated
     *
     */
    @Test
    void testFindByNameExactMatch() {
        Incident incident1 = new Incident();
        incident1.setIncidentId("incident1");
        incident1.setLatitude("30.12345");
        incident1.setLongitude("-70.98765");
        incident1.setNumberOfPeople(3);
        incident1.setMedicalNeeded(true);
        incident1.setVictimName("John Doe");
        incident1.setVictimPhoneNumber("(111) 456-78990");
        incident1.setReportedTime(Instant.now());
        incident1.setStatus("REPORTED");

        Incident incident2 = new Incident();
        incident2.setIncidentId("incident2");
        incident2.setLatitude("31.12345");
        incident2.setLongitude("-71.98765");
        incident2.setNumberOfPeople(4);
        incident2.setMedicalNeeded(true);
        incident2.setVictimName("Jane Foo");
        incident2.setVictimPhoneNumber("(211) 456-78990");
        incident2.setReportedTime(Instant.now());
        incident2.setStatus("ASSIGNED");

        Incident incident3 = new Incident();
        incident3.setIncidentId("incident3");
        incident3.setLatitude("32.12345");
        incident3.setLongitude("-72.98765");
        incident3.setNumberOfPeople(5);
        incident3.setMedicalNeeded(true);
        incident3.setVictimName("Fred Who");
        incident3.setVictimPhoneNumber("(311) 456-78990");
        incident3.setReportedTime(Instant.now());
        incident3.setStatus("RESCUED");

        createIncidents(Arrays.asList(incident1, incident2, incident3));

        List<Incident> incidents = new TransactionTemplate(transaction).execute(() ->incidentDao.findByName("Jane Foo"));
        assertThat(incidents.size(), equalTo(1));
        Incident matched = incidents.get(0);
        assertThat(matched.getIncidentId(), equalTo(incident2.getIncidentId()));
        assertThat(matched.getVictimName(), equalTo(incident2.getVictimName()));
        assertThat(matched.getVictimPhoneNumber(), equalTo(incident2.getVictimPhoneNumber()));
        assertThat(matched.getLatitude(), equalTo(incident2.getLatitude()));
        assertThat(matched.getLongitude(), equalTo(incident2.getLongitude()));
        assertThat(matched.getNumberOfPeople(), equalTo(incident2.getNumberOfPeople()));
        assertThat(matched.isMedicalNeeded(), equalTo(incident2.isMedicalNeeded()));
        assertThat(matched.getReportedTime(), equalTo(incident2.getReportedTime()));
        assertThat(matched.getStatus(), equalTo(incident2.getStatus()));
    }

    /**
     *  Test description:
     *
     *    When:
     *      There are Incident records in the database
     *      There is several Incidents with the name which matches the given name pattern
     *      A call is made to `findByName`
     *
     *    Then:
     *      A list of Incident objects is returned
     *      The name of the found Incidents matches the given name pattern
     *
     */
    @Test
    void testFindByNamePartialMatchSeveralIncidents() {
        Incident incident1 = new Incident();
        incident1.setIncidentId("incident1");
        incident1.setLatitude("30.12345");
        incident1.setLongitude("-70.98765");
        incident1.setNumberOfPeople(3);
        incident1.setMedicalNeeded(true);
        incident1.setVictimName("Jane Doe");
        incident1.setVictimPhoneNumber("(111) 456-78990");
        incident1.setReportedTime(Instant.now());
        incident1.setStatus("REPORTED");

        Incident incident2 = new Incident();
        incident2.setIncidentId("incident2");
        incident2.setLatitude("31.12345");
        incident2.setLongitude("-71.98765");
        incident2.setNumberOfPeople(4);
        incident2.setMedicalNeeded(true);
        incident2.setVictimName("Jane Foo");
        incident2.setVictimPhoneNumber("(211) 456-78990");
        incident2.setReportedTime(Instant.now());
        incident2.setStatus("ASSIGNED");

        Incident incident3 = new Incident();
        incident3.setIncidentId("incident3");
        incident3.setLatitude("32.12345");
        incident3.setLongitude("-72.98765");
        incident3.setNumberOfPeople(5);
        incident3.setMedicalNeeded(true);
        incident3.setVictimName("Fred Who");
        incident3.setVictimPhoneNumber("(311) 456-78990");
        incident3.setReportedTime(Instant.now());
        incident3.setStatus("RESCUED");

        createIncidents(Arrays.asList(incident1, incident2, incident3));

        List<Incident> incidents = new TransactionTemplate(transaction).execute(() ->incidentDao.findByName("Jane%"));
        assertThat(incidents.size(), equalTo(2));
        incidents.forEach(i -> assertThat(i.getVictimName(), startsWith("Jane")));
    }

    @Test
    void testDeleteAll() {
        createIncidents();
        assertThat(getAllIncidents().size(), equalTo(3));
        new TransactionTemplate(transaction).execute(() -> {
            incidentDao.deleteAll();
            return null;
        });
        assertThat(getAllIncidents().size(), equalTo(0));
    }

    void createIncidents() {
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

        createIncidents(Arrays.asList(incident1, incident2, incident3));
    }

    @Transactional
    List<?> getAllIncidents() {
        return entityManager.createQuery("SELECT i FROM Incident i").getResultList();
    }

    @Transactional
    void createIncidents(List<Incident> incidents) {
        incidents.forEach(i -> entityManager.persist(i));
    }

    public static class TransactionTemplate {

        private final UserTransaction transaction;

        public TransactionTemplate(UserTransaction transaction) {
            this.transaction = transaction;
        }

        public <T> T execute(TransactionCallback<T> action) {
            try {
                transaction.begin();
                T result = action.doInTransaction();
                transaction.commit();
                return result;
            } catch (Exception e) {
                try {
                    transaction.rollback();
                } catch (Exception systemException) {
                    //ignore;
                }
                return null;
            }
        }
    }

    @FunctionalInterface
    public interface TransactionCallback<T> {

        T doInTransaction();

    }

}
