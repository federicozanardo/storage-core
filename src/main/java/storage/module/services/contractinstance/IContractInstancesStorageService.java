package storage.module.services.contractinstance;

import lcp.lib.models.contract.ContractInstance;
import storage.exceptions.ContractInstanceNotFoundException;

import java.io.IOException;
import java.util.ArrayList;

public interface IContractInstancesStorageService {
    String saveContractInstance(ContractInstance contractInstance) throws IOException;

    ContractInstance getContractInstance(String contractInstanceId) throws IOException, ContractInstanceNotFoundException;

    /*void storeGlobalSpace(String contractInstanceId, HashMap<String, TraceChange> updates)
            throws IOException,
            ContractInstanceNotFoundException;*/

    void storeStateMachine(String contractInstanceId, String partyName, String functionName, ArrayList<String> argumentsTypes)
            throws IOException,
            ContractInstanceNotFoundException;

    void storeStateMachine(String contractInstanceId, String obligationFunctionName)
            throws IOException,
            ContractInstanceNotFoundException;
}
