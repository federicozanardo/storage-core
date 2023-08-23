package storage.exceptions;

import storage.core.lib.exceptions.database.DatabaseException;

public class RocksDBDatabaseException extends DatabaseException {
    private static final String databaseName = "RocksDB";

    public RocksDBDatabaseException(String message) {
        super(databaseName, message);
    }

    public RocksDBDatabaseException(String message, Exception exception) {
        super(databaseName, message, exception);
    }
}
