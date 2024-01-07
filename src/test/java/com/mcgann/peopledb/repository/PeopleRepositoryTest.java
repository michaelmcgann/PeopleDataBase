package com.mcgann.peopledb.repository;

import com.mcgann.peopledb.model.Address;
import com.mcgann.peopledb.model.Person;
import com.mcgann.peopledb.model.Region;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTest {

    private Connection connection;
    private PeopleRepository repo;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("PUT DATABASE PATH HERE");
        connection.setAutoCommit(false);
        repo = new PeopleRepository(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    public void canSaveOnePerson() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));
        Person bobby = new Person("Bobby", "Smith", ZonedDateTime.of(1980, 11,
                15, 0, 0, 0, 0, ZoneId.of("-8")));
        Person savedPersonOne = repo.save(john);
        Person savedPersonTwo = repo.save(bobby);
        assertThat(savedPersonOne.getId()).isNotEqualTo(savedPersonTwo.getId());
    }

    @Test
    public void canSavePersonWithHomeAddress() throws SQLException {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null,"123 Birch Street", "Apt 1A", "Leeds", "WA", "90210", "United States",
                "Fulton County", Region.WEST);
        john.setHomeAddress(address);

        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getHomeAddress().get().id()).isGreaterThan(0);
    }

    @Test
    public void canSavePersonBusinessWithAddress() throws SQLException {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null,"123 Birch Street", "Apt 1A", "Leeds", "WA", "90210", "United States",
                "Fulton County", Region.WEST);
        john.setBusinessAddress(address);

        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getBusinessAddress().get().id()).isGreaterThan(0);
    }

    @Test void canSavePersonWithChildren() throws SQLException {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));
        Person johnny = new Person("Johnny", "Smith", ZonedDateTime.of(2000, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));
        Person bobby = new Person("Bobby", "Smith", ZonedDateTime.of(2000, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));
        Person tommy = new Person("Tommy", "Smith", ZonedDateTime.of(2000, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));

        john.addChild(johnny);
        john.addChild(bobby);
        john.addChild(tommy);
        Person savedJohn = repo.save(john);
        savedJohn.getChildren().stream()
                        .map(Person::getId)
                                .forEach(id -> assertThat(id).isGreaterThan(0));

//        connection.commit();
    }

    @Test
    public void canSavePersonWithSpouse() throws SQLException {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null,"123 Birch Street", "Apt 1A", "Leeds", "WA", "90210", "United States",
                "Fulton County", Region.WEST);
        john.setHomeAddress(address);

        Person bobby = new Person("Bobby", "Smith", ZonedDateTime.of(1980, 11,
                15, 0, 0, 0, 0, ZoneId.of("-8")));
        bobby.setHomeAddress(address);

        Person savedJohn = repo.save(john);
        Person savedBobby = repo.save(bobby);

        john.setSpouseId(bobby.getId());
        bobby.setSpouseId(john.getId());


        assertThat(savedJohn.getSpouseId().get()).isEqualTo(savedBobby.getId());
    }

    @Test
    public void canFindPersonById() {
        Person savedPerson = repo.save(new Person("test", "123", ZonedDateTime.now()));
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void canFindPersonByIdWithHomeAddress() throws SQLException {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null,"123 Birch Street", "Apt 1A", "Leeds", "WA", "90210", "United States",
                "Fulton County", Region.WEST);
        john.setHomeAddress(address);

        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getHomeAddress().get().state()).isEqualTo("WA");
    }

    @Test
    public void canFindPersonByIdWithBusinessAddress() throws SQLException {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null,"123 Birch Street", "Apt 1A", "Leeds", "WA", "90210", "United States",
                "Fulton County", Region.WEST);
        john.setBusinessAddress(address);

        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getBusinessAddress().get().state()).isEqualTo("WA");
    }

    @Test
    public void canFindPersonByIdWithChildren() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));
        Person johnny = new Person("Johnny", "Smith", ZonedDateTime.of(2000, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));
        Person bobby = new Person("Bobby", "Smith", ZonedDateTime.of(2000, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));
        Person tommy = new Person("Tommy", "Smith", ZonedDateTime.of(2000, 11,
                15, 0, 0, 0, 0, ZoneId.of("-6")));

        john.addChild(johnny);
        john.addChild(bobby);
        john.addChild(tommy);
        Person savedJohn = repo.save(john);
        Person fetchedJohn = repo.findById(savedJohn.getId()).get();

        assertThat(fetchedJohn.getChildren().stream().map(Person::getFirstName).collect(Collectors.toSet()))
                .contains("Johnny", "Bobby", "Tommy");
    }

    @Test
    public void testPersonIdNotFound() {
        Optional<Person> foundPerson = repo.findById(-1L);
        assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canCount() {
        long startCount = repo.count();
        repo.save(new Person("test", "123", ZonedDateTime.now()));
        repo.save(new Person("test", "123", ZonedDateTime.now()));
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount + 2);
    }

    @Test
    public void canDelete() {
        Person savedPerson = repo.save(new Person("test", "123", ZonedDateTime.now()));
        long startCount = repo.count();
        repo.delete(savedPerson);
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount - 1);
    }

    @Test
    public void canDeleteMultiplePeople() {
        Person person1 = repo.save(new Person("test", "123", ZonedDateTime.now()));
        Person person2 = repo.save(new Person("test", "123", ZonedDateTime.now()));
        long startCount = repo.count();
        repo.delete(person1, person2);
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount - 2);
    }

    @Test
    public void canUpdate() {
        Person savedPerson = repo.save(new Person("test", "123", ZonedDateTime.now()));

        Person p1 = repo.findById(savedPerson.getId()).get();

        savedPerson.setSalary(new BigDecimal("73000.23"));
        repo.update(savedPerson);

        Person p2 = repo.findById(savedPerson.getId()).get();

        assertThat(p2.getSalary()).isNotEqualByComparingTo(p1.getSalary());
    }
}
