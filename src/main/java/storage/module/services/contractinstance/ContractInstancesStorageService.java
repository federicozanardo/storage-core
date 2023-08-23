package storage.module.services.contractinstance;

import lcp.lib.models.contract.ContractInstance;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import storage.core.lib.exceptions.database.DatabaseException;
import storage.core.lib.exceptions.services.contractinstance.ContractInstanceNotFoundException;
import storage.core.lib.module.services.IContractInstancesStorageService;
import storage.exceptions.RocksDBDatabaseException;
import storage.module.services.StorageSerializer;
import storage.utils.RocksDBUtils;

import java.util.ArrayList;
import java.util.UUID;

import static org.rocksdb.util.ByteUtil.bytes;

public class ContractInstancesStorageService extends StorageSerializer<ContractInstance> implements IContractInstancesStorageService {
    public RocksDB db;

    public ContractInstancesStorageService() {
        RocksDB.loadLibrary();
    }

    public ContractInstance getContractInstance(String contractInstanceId) throws ContractInstanceNotFoundException, DatabaseException {
        db = RocksDBUtils.open();

        ContractInstance contractInstance;
        try {
            contractInstance = this.deserialize(db.get(bytes(contractInstanceId)));
        } catch (RocksDBException e) {
            throw new RocksDBDatabaseException("Error while reading from database", e);
        } finally {
            db.close();
        }

        if (contractInstance == null) {
            throw new ContractInstanceNotFoundException(contractInstanceId);
        }

        return contractInstance;
    }

    public String saveContractInstance(ContractInstance contractInstance) throws DatabaseException {
        db = RocksDBUtils.open();

        // TODO: check that the id is unique
        String contractInstanceId = UUID.randomUUID().toString();
        try {
            db.put(bytes(contractInstanceId), this.serialize(contractInstance));
        } catch (RocksDBException e) {
            throw new RocksDBDatabaseException("Error while writing to database", e);
        } finally {
            db.close();
        }

        return contractInstanceId;
    }

    // FIXME
    /*public void storeGlobalSpace(String contractInstanceId, HashMap<String, TraceChange> updates)
            throws IOException,
            ContractInstanceNotFoundException {
        mutex.lock();
        levelDb = factory.open(new File(String.valueOf(Constants.CONTRACT_INSTANCES_PATH)), new Options());

        ContractInstance contractInstance = this.deserialize(levelDb.get(bytes(contractInstanceId)));
        if (contractInstance == null) {
            levelDb.close();
            mutex.unlock();
            throw new ContractInstanceNotFoundException(contractInstanceId);
        }

        for (HashMap.Entry<String, TraceChange> entry : updates.entrySet()) {
            String variableName = entry.getKey();
            TraceChange value = entry.getValue();

            if (value.isChanged()) {
                contractInstance.getGlobalSpace().put(variableName, value.getValue());
            }
        }

        levelDb.put(bytes(contractInstanceId), this.serialize(contractInstance));

        levelDb.close();
        mutex.unlock();
    }*/

    // FIXME: return a boolean (true --> success, false --> otherwise)

    public void saveStateMachine(String contractInstanceId, String partyName, String functionName, ArrayList<String> argumentsTypes)
            throws ContractInstanceNotFoundException,
            DatabaseException {
        db = RocksDBUtils.open();

        ContractInstance contractInstance;
        try {
            contractInstance = this.deserialize(db.get(bytes(contractInstanceId)));
        } catch (RocksDBException e) {
            throw new RocksDBDatabaseException("Error while reading from database", e);
        }

        if (contractInstance == null) {
            db.close();
            throw new ContractInstanceNotFoundException(contractInstanceId);
        }

        // FIXME
        contractInstance.getStateMachine().nextState(partyName, functionName, argumentsTypes);
        try {
            db.put(bytes(contractInstanceId), this.serialize(contractInstance));
        } catch (RocksDBException e) {
            throw new RocksDBDatabaseException("Error while writing to database", e);
        } finally {
            db.close();
        }
    }

    // FIXME: return a boolean (true --> success, false --> otherwise)

    public void saveStateMachine(String contractInstanceId, String obligationFunctionName)
            throws ContractInstanceNotFoundException,
            DatabaseException {
        db = RocksDBUtils.open();

        ContractInstance contractInstance;
        try {
            contractInstance = this.deserialize(db.get(bytes(contractInstanceId)));
        } catch (RocksDBException e) {
            throw new RocksDBDatabaseException("Error while reading from database", e);
        }

        if (contractInstance == null) {
            db.close();
            throw new ContractInstanceNotFoundException(contractInstanceId);
        }

        // FIXME
        contractInstance.getStateMachine().nextState(obligationFunctionName);
        try {
            db.put(bytes(contractInstanceId), this.serialize(contractInstance));
        } catch (RocksDBException e) {
            throw new RocksDBDatabaseException("Error while writing to database", e);
        } finally {
            db.close();
        }
    }
}
