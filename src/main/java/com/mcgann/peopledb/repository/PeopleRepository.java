package com.mcgann.peopledb.repository;

import com.mcgann.peopledb.annotation.SQL;
import com.mcgann.peopledb.model.Address;
import com.mcgann.peopledb.model.CrudOperation;
import com.mcgann.peopledb.model.Person;
import com.mcgann.peopledb.model.Region;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;


public class PeopleRepository extends CRUDRepository<Person> {
    private AddressRepository addressRepository;
    public static final String SAVE_PERSON_SQL = """
        INSERT INTO PEOPLE
        (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS, BUSINESS_ADDRESS, SPOUSE, PARENT_ID)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""";
    public static final String FIND_BY_ID_SQL = """
            SELECT
            PARENT.ID AS PARENT_ID, PARENT.FIRST_NAME AS PARENT_FIRST_NAME, PARENT.LAST_NAME AS PARENT_LAST_NAME,
            PARENT.DOB AS PARENT_DOB, PARENT.SALARY AS PARENT_SALARY, PARENT.EMAIL AS PARENT_EMAIL,
            PARENT.SPOUSE AS SPOUSE,
            
            CHILD.ID AS CHILD_ID, CHILD.FIRST_NAME AS CHILD_FIRST_NAME, CHILD.LAST_NAME AS CHILD_LAST_NAME,
            CHILD.DOB AS CHILD_DOB, CHILD.SALARY AS CHILD_SALARY, CHILD.EMAIL AS CHILD_EMAIL,
            
            HOME.ID AS HOME_ID, HOME.STREET_ADDRESS AS HOME_STREET_ADDRESS, HOME.ADDRESS2 AS HOME_ADDRESS2,
            HOME.STATE AS HOME_STATE,
            HOME.CITY AS HOME_CITY, HOME.POSTCODE AS HOME_POSTCODE, HOME.COUNTY AS HOME_COUNTY,
            HOME.REGION AS HOME_REGION, HOME.COUNTRY AS HOME_COUNTRY,
            
            BUSINESS.ID AS BUSINESS_ID, BUSINESS.STREET_ADDRESS AS BUSINESS_STREET_ADDRESS,
            BUSINESS.ADDRESS2 AS BUSINESS_ADDRESS2, BUSINESS.STATE AS BUSINESS_STATE,
            BUSINESS.CITY AS BUSINESS_CITY, BUSINESS.POSTCODE AS BUSINESS_POSTCODE, BUSINESS.COUNTY AS BUSINESS_COUNTY,
            BUSINESS.REGION AS BUSINESS_REGION, BUSINESS.COUNTRY AS BUSINESS_COUNTRY
            
            FROM PEOPLE AS PARENT
            
            LEFT OUTER JOIN PEOPLE AS CHILD ON PARENT.ID = CHILD.PARENT_ID
            LEFT OUTER JOIN ADDRESSES AS HOME ON PARENT.HOME_ADDRESS = HOME.ID
            LEFT OUTER JOIN ADDRESSES AS BUSINESS ON PARENT.BUSINESS_ADDRESS = BUSINESS.ID
            
            WHERE PARENT.ID = ?
            """;
    public static final String SQL_COUNT_ALL = "SELECT COUNT(*) FROM PEOPLE";
    public static final String DELETE_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    public static final String DELETE_IN_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    public static final String UPDATE_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?," +
            " SALARY=? WHERE ID=?";

    public PeopleRepository(Connection connection) {
        super(connection);
        addressRepository = new AddressRepository(connection);
    }

    @Override
    @SQL(value = SAVE_PERSON_SQL, operationType = CrudOperation.SAVE)
    void mapForSave(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimeStamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
        ps.setString(5, entity.getEmail());
        Address savedAddress;
        associateAddressWithPerson(ps, entity.getHomeAddress(), 6);
        associateAddressWithPerson(ps, entity.getBusinessAddress(), 7);
        associateSpouseWithPerson(ps, entity.getSpouseId(), 8);
        associateParentWithPerson(entity, ps);
    }

    private static void associateParentWithPerson(Person entity, PreparedStatement ps) throws SQLException {
        if (entity.getParent().isPresent()) {
            ps.setLong(9, entity.getParent().get().getId());
        }
        else { ps.setObject(9, null); }
    }

    @Override
    protected void postSave(Person entity, long id) {
        entity.getChildren().stream()
                .forEach(this::save);
    }

    private void associateSpouseWithPerson(PreparedStatement ps, Optional<Long> spouseId, int parameterIndex) throws SQLException {
        if (spouseId.isPresent()) {
            ps.setLong(parameterIndex, spouseId.orElse(0L));
        } else {
            ps.setObject(parameterIndex, null);
        }
    }

    private void associateAddressWithPerson(PreparedStatement ps, Optional<Address> address, int parameterIndex) throws SQLException {
        Address savedAddress;
        if (address.isPresent()) {
            savedAddress = addressRepository.save(address.get());
            ps.setLong(parameterIndex, savedAddress.id());
        } else {
            ps.setObject(parameterIndex, null);
        }
    }

    @Override
    @SQL(value = UPDATE_SQL, operationType = CrudOperation.UPDATE)
    void mapForUpdate(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimeStamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
    }

    @Override
    @SQL(value = FIND_BY_ID_SQL, operationType = CrudOperation.FIND_BY_ID)
    Person extractEntityFromResultsSet(ResultSet rs) throws SQLException{
        Person parent = extractPerson(rs, "PARENT_");
        Address homeAddress = extractAddress(rs, "HOME_");
        Address businessAddress = extractAddress(rs, "BUSINESS_");
        long spouseId = rs.getLong("SPOUSE");

        parent.setHomeAddress(homeAddress);
        parent.setBusinessAddress(businessAddress);
        parent.setSpouseId(spouseId);

        do {
            rs.getLong("CHILD_ID");
            if (!rs.wasNull()) {
                Person foundChild = extractPerson(rs, "CHILD_");
                parent.addChild(foundChild);
            }
        } while (rs.next());
        return parent;
    }

    private static Person extractPerson(ResultSet rs, String aliasPrefix) throws SQLException {
        Person foundPerson;
        long personId = rs.getLong(aliasPrefix + "ID");
        String firstName = rs.getString(aliasPrefix + "FIRST_NAME");
        String lastName = rs.getString(aliasPrefix + "LAST_NAME");
        ZonedDateTime dob = ZonedDateTime.of(rs.getTimestamp(aliasPrefix + "DOB").toLocalDateTime(),
                ZoneId.of("+0"));
        BigDecimal salary = rs.getBigDecimal(aliasPrefix + "SALARY");
//        long homeAddressId = rs.getLong(aliasPrefix + "HOME_ADDRESS");
        foundPerson = new Person(firstName, lastName, dob);
        foundPerson.setId(personId);
        foundPerson.setSalary(salary);
        return foundPerson;
    }

    private static Address extractAddress(ResultSet rs, String addressType) throws SQLException {
        long addressId = rs.getLong(addressType + "ID");
        if (rs.getObject(addressType + "ID") == null) { return null; }
        String streetAddress = rs.getString(addressType + "STREET_ADDRESS");
        String address2 = rs.getString(addressType + "ADDRESS2");
        String city = rs.getString(addressType + "CITY");
        String state = rs.getString(addressType + "STATE");
        String postcode = rs.getString(addressType + "POSTCODE");
        String county = rs.getString(addressType + "COUNTY");
        Region region = Region.valueOf(rs.getString(addressType + "REGION").toUpperCase());
        String country = rs.getString(addressType + "COUNTRY");
        return new Address(addressId, streetAddress, address2, city, state, postcode, country, county, region);
    }

    @Override
    protected String getFindByIdSql() {
        return FIND_BY_ID_SQL;
    }

    @Override
    protected String getCountSql() {
        return SQL_COUNT_ALL;
    }

    @Override
    protected String getDeleteSql() {
        return DELETE_SQL;
    }

    @Override
    protected String getDeleteInSql() {
        return DELETE_IN_SQL;
    }

    private static Timestamp convertDobToTimeStamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
//    Person finalParent = null;
//        do {
//                Person currentParent = extractPerson(rs, "PARENT_");
//                if (finalParent == null) { finalParent = currentParent; }
//
//                Person foundChild = extractPerson(rs, "CHILD_");
//
//                Address homeAddress = extractAddress(rs, "HOME_");
//                Address businessAddress = extractAddress(rs, "BUSINESS_");
//                long spouseId = rs.getLong("SPOUSE");
//
//                finalParent.setHomeAddress(homeAddress);
//                finalParent.setBusinessAddress(businessAddress);
//                finalParent.setSpouseId(spouseId);
//                } while (rs.next());
//                return finalParent;
//                }


