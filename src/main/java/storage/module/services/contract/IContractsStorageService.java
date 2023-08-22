package storage.module.services.contract;

import lcp.lib.models.contract.Contract;
import storage.exceptions.ContractNotFoundException;

import java.io.IOException;

public interface IContractsStorageService {
    String saveContract(Contract contract) throws IOException;

    Contract getContract(String contractId) throws IOException, ContractNotFoundException;
}
