package storage.utils;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import storage.constants.Constants;
import storage.exceptions.RocksDBDatabaseException;

public class RocksDBUtils {
    public static RocksDB open(Options options, String path) throws RocksDBDatabaseException {
        RocksDB db;

        try {
            db = RocksDB.open(options, path);
        } catch (RocksDBException e) {
            throw new RocksDBDatabaseException("Error while opening the database", e);
        }

        return db;
    }

    public static RocksDB open(Options options) throws RocksDBDatabaseException {
        RocksDB db;

        try {
            db = RocksDB.open(options, String.valueOf(Constants.ASSETS_PATH));
        } catch (RocksDBException e) {
            throw new RocksDBDatabaseException("Error while opening the database", e);
        }

        return db;
    }

    public static RocksDB open() throws RocksDBDatabaseException {
        RocksDB db;

        try {
            db = RocksDB.open(new Options(), String.valueOf(Constants.ASSETS_PATH));
        } catch (RocksDBException e) {
            throw new RocksDBDatabaseException("Error while opening the database", e);
        }

        return db;
    }
}
