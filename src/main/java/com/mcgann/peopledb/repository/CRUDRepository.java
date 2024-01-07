package com.mcgann.peopledb.repository;

import com.mcgann.peopledb.annotation.Id;
import com.mcgann.peopledb.annotation.SQL;
import com.mcgann.peopledb.exception.UnableToSaveException;
import com.mcgann.peopledb.model.CrudOperation;

import java.sql.*;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

abstract class CRUDRepository <T> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    public T save(T entity) throws UnableToSaveException {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.SAVE,
                            this::getSaveSql), Statement.RETURN_GENERATED_KEYS);

            mapForSave(entity, ps);

            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            System.out.printf("Records affected: %d%n", recordsAffected);

            while (rs.next()) {
                long id = rs.getLong(1);
                setIdByAnnotation(id, entity);
                postSave(entity, id);
                System.out.println(entity);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Failed to save entity: " + entity);
        }
        return entity;
    }

    public Optional<T> findById(Long id) {
        T foundEntity = null;

        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_BY_ID,
                    this::getFindByIdSql));
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                foundEntity = extractEntityFromResultsSet(rs);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Optional.ofNullable(foundEntity);
    }

    public long count() {
        long count = 0;
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.COUNT,
                    this::getCountSql));
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                count = rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    public void delete(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.DELETE_ONE,
                    this::getDeleteSql));
            ps.setLong(1, getIdByAnnotation(entity));
            int affectedRecordCount = ps.executeUpdate();
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setIdByAnnotation (Long id, T entity) {
        Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .forEach(f -> {
                    f.setAccessible(true);
                    try {
                        f.set(entity, id);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Unable to set ID field value.");
                    }
                });
    }

    private Long getIdByAnnotation(T entity) {
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .map(f -> {
                    f.setAccessible(true);
                    Long id;
                    try {
                        id = (long)f.get(entity);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    return id;
                })
                .findFirst().orElseThrow(() -> new RuntimeException("No ID annotated field found"));
    }

    public void delete(T...entities) {
        try {
            Statement stmt = connection.createStatement();
            String ids = Arrays.stream(entities).map(this::getIdByAnnotation).map(String::valueOf).collect(joining(","));
            int affectedRecordCount = stmt.executeUpdate(getSqlByAnnotation(CrudOperation.DELETE_MANY,
                    this::getDeleteInSql).replace(":ids",
                    ids));
            System.out.println(affectedRecordCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.UPDATE,
                    this::getUpdateSql));
            mapForUpdate(entity, ps);
            ps.setLong(5, getIdByAnnotation(entity));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getSqlByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter) {
        return Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(SQL.class))
                .map(m -> m.getAnnotation(SQL.class))
                .filter(a -> a.operationType().equals(operationType))
                .map(SQL::value)
                .findFirst().orElseGet(sqlGetter);
    }

    protected void postSave(T entity, long id) {}

    protected String getUpdateSql() {throw new RuntimeException("SQL not defined.");}

    /**
     *
     * @return Should return SQL String like "DELETE FROM PEOPLE WHERE ID IN (:ids)"
     * Be sure to include the "(:ids)" named parameter and call it "ids"
     */
    protected String getDeleteInSql() {throw new RuntimeException("SQL not defined.");}

    protected String getDeleteSql() {throw new RuntimeException("SQL not defined.");}

    protected String getCountSql() {throw new RuntimeException("SQL not defined.");}

    String getSaveSql() {return "";}

    /**
     *
     * @return Returns a String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", that will bind to the entity's ID.
     */
    protected String getFindByIdSql() {return "";}
    abstract T extractEntityFromResultsSet(ResultSet rs) throws SQLException;

    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

}
