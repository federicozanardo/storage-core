package storage.module.services.contract;

import lcp.lib.models.contract.Contract;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import storage.constants.Constants;
import storage.core.lib.exceptions.database.DatabaseException;
import storage.core.lib.exceptions.services.contract.ContractNotFoundException;
import storage.core.lib.module.services.IContractsStorageService;
import storage.exceptions.RocksDBDatabaseException;
import storage.module.services.StorageSerializer;
import storage.utils.RocksDBUtils;

import java.util.UUID;

import static org.rocksdb.util.ByteUtil.bytes;

public class ContractsStorageService extends StorageSerializer<Contract> implements IContractsStorageService {
    private RocksDB db;

    public ContractsStorageService() {
        RocksDB.loadLibrary();
    }

    public Contract getContract(String contractId) throws ContractNotFoundException, DatabaseException {
        db = RocksDBUtils.open(String.valueOf(Constants.CONTRACTS_PATH));

        Contract contract;
        try {
            contract = this.deserialize(db.get(bytes(contractId)));
        } catch (RocksDBException e) {
            throw new RocksDBDatabaseException("Error while reading from database", e);
        } finally {
            db.close();
        }

        if (contract == null) {
            throw new ContractNotFoundException(contractId);
        }

        return contract;
    }

    public String saveContract(Contract contract) throws DatabaseException {
        db = RocksDBUtils.open(String.valueOf(Constants.CONTRACTS_PATH));

        // TODO: check that the id is unique
        String contractId = UUID.randomUUID().toString();
        try {
            db.put(bytes(contractId), this.serialize(contract));
        } catch (RocksDBException e) {
            throw new RocksDBDatabaseException("Error while writing to database", e);
        } finally {
            db.close();
        }

        return contractId;
    }
}
