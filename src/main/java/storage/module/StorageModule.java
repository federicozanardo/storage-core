package storage.module;

import lcp.lib.communication.module.Module;
import lcp.lib.communication.module.channel.ChannelMessage;
import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lcp.lib.communication.module.channel.ModuleChannel;
import lcp.lib.communication.module.channel.responses.RequestNotFound;
import lcp.lib.models.assets.Asset;
import lcp.lib.models.contract.Contract;
import lcp.lib.models.contract.ContractInstance;
import lcp.lib.models.ownership.Ownership;
import lcp.lib.models.singleuseseal.SingleUseSeal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.core.lib.exceptions.AssetNotFoundException;
import storage.core.lib.exceptions.ContractInstanceNotFoundException;
import storage.core.lib.exceptions.ContractNotFoundException;
import storage.core.lib.exceptions.OwnershipNotFoundException;
import storage.core.lib.exceptions.OwnershipsNotFoundException;
import storage.core.lib.models.dto.asset.getassetinfo.GetAssetInfoRequest;
import storage.core.lib.models.dto.asset.getassetinfo.GetAssetInfoResponse;
import storage.core.lib.models.dto.contract.getcontract.GetContractRequest;
import storage.core.lib.models.dto.contract.getcontract.GetContractResponse;
import storage.core.lib.models.dto.contract.savecontract.SaveContractRequest;
import storage.core.lib.models.dto.contract.savecontract.SaveContractResponse;
import storage.core.lib.models.dto.contractinstance.getcontractinstance.GetContractInstanceRequest;
import storage.core.lib.models.dto.contractinstance.getcontractinstance.GetContractInstanceResponse;
import storage.core.lib.models.dto.contractinstance.savecontractinstance.SaveContractInstanceRequest;
import storage.core.lib.models.dto.contractinstance.savecontractinstance.SaveContractInstanceResponse;
import storage.core.lib.models.dto.contractinstance.storestatemachine.function.StoreStateMachineFromFunctionCallRequest;
import storage.core.lib.models.dto.contractinstance.storestatemachine.function.StoreStateMachineFromFunctionCallResponse;
import storage.core.lib.models.dto.contractinstance.storestatemachine.obligation.StoreStateMachineFromObligationCallRequest;
import storage.core.lib.models.dto.contractinstance.storestatemachine.obligation.StoreStateMachineFromObligationCallResponse;
import storage.core.lib.models.dto.ownership.addfunds.AddFundsRequest;
import storage.core.lib.models.dto.ownership.addfunds.AddFundsResponse;
import storage.core.lib.models.dto.ownership.getfund.GetFundRequest;
import storage.core.lib.models.dto.ownership.getfund.GetFundResponse;
import storage.core.lib.models.dto.ownership.getfunds.GetFundsRequest;
import storage.core.lib.models.dto.ownership.getfunds.GetFundsResponse;
import storage.core.lib.models.dto.ownership.makeownershipspent.MakeOwnershipSpentRequest;
import storage.core.lib.models.dto.ownership.makeownershipspent.MakeOwnershipSpentResponse;
import storage.module.services.asset.AssetsStorageService;
import storage.module.services.contract.ContractsStorageService;
import storage.module.services.contractinstance.ContractInstancesStorageService;
import storage.module.services.ownership.OwnershipsStorageService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class StorageModule extends Module {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final AssetsStorageService assetsStorageService;
    private final ContractInstancesStorageService contractInstancesStorageService;
    private final ContractsStorageService contractsStorageService;
    private final OwnershipsStorageService ownershipsStorageService;

    public StorageModule() {
        this.assetsStorageService = new AssetsStorageService();
        this.contractInstancesStorageService = new ContractInstancesStorageService();
        this.contractsStorageService = new ContractsStorageService();
        this.ownershipsStorageService = new OwnershipsStorageService();
    }

    @Override
    public void send(String receiverModuleId, ChannelMessagePayload payload) {
        logger.debug("[{}] payload: {}", new Object() {
        }.getClass().getEnclosingMethod().getName(), payload);
        ModuleChannel channel = this.findChannel(this.getId(), receiverModuleId);

        if (channel != null) {
            channel.send(new ChannelMessage(this.getId(), payload));
        } else {
            logger.error("Impossible to find a channel with {}!", receiverModuleId);
        }
    }

    @Override
    public void receive(ChannelMessage message) {
        logger.debug("[{}] from: {}, payload: {}", new Object() {
        }.getClass().getEnclosingMethod().getName(), message.getSenderModuleId(), message.getPayload());
    }

    @Override
    public ChannelMessage sendAndReceive(String receiverModuleId, ChannelMessagePayload payload) {
        logger.debug("[{}] payload: {}", new Object() {
        }.getClass().getEnclosingMethod().getName(), payload);
        ModuleChannel channel = this.findChannel(this.getId(), receiverModuleId);

        if (channel != null) {
            return channel.sendAndReceive(new ChannelMessage(this.getId(), payload));
        } else {
            logger.error("Impossible to find a channel with {}!", receiverModuleId);
            return null;
        }
    }

    @Override
    public ChannelMessage receiveAndResponse(ChannelMessage message) {
        logger.debug("[{}] from: {}, payload: {}", new Object() {
        }.getClass().getEnclosingMethod().getName(), message.getSenderModuleId(), message.getPayload());
        ChannelMessagePayload payload = message.getPayload();

        if (payload instanceof GetAssetInfoRequest) {
            try {
                Asset assetInfo = assetsStorageService.getAssetInfo(((GetAssetInfoRequest) payload).getAssetId());
                return new ChannelMessage(this.getId(), new GetAssetInfoResponse(assetInfo));
            } catch (IOException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            } catch (AssetNotFoundException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            }
        } else if (payload instanceof SaveContractRequest) {
            Contract contractToSave = ((SaveContractRequest) payload).getContract();
            try {
                String contractId = contractsStorageService.saveContract(contractToSave);
                return new ChannelMessage(this.getId(), new SaveContractResponse(contractId));
            } catch (IOException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            }
        } else if (payload instanceof GetContractRequest) {
            String contractId = ((GetContractRequest) payload).getContractId();
            try {
                Contract contract = contractsStorageService.getContract(contractId);
                return new ChannelMessage(this.getId(), new GetContractResponse(contract));
            } catch (IOException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            } catch (ContractNotFoundException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            }
        } else if (payload instanceof SaveContractInstanceRequest) {
            ContractInstance contractInstance = ((SaveContractInstanceRequest) payload).getContractInstance();
            try {
                String contractInstanceId = contractInstancesStorageService.saveContractInstance(contractInstance);
                return new ChannelMessage(this.getId(), new SaveContractInstanceResponse(contractInstanceId));
            } catch (IOException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            }
        } else if (payload instanceof GetContractInstanceRequest) {
            String contractInstanceId = ((GetContractInstanceRequest) payload).getContractInstanceId();
            try {
                ContractInstance contractInstance = contractInstancesStorageService.getContractInstance(contractInstanceId);
                return new ChannelMessage(this.getId(), new GetContractInstanceResponse(contractInstance));
            } catch (IOException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            } catch (ContractInstanceNotFoundException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            }
        } else if (payload instanceof StoreStateMachineFromFunctionCallRequest) {
            String contractInstanceId = ((StoreStateMachineFromFunctionCallRequest) payload).getContractInstanceId();
            String partyName = ((StoreStateMachineFromFunctionCallRequest) payload).getPartyName();
            String functionName = ((StoreStateMachineFromFunctionCallRequest) payload).getFunctionName();
            ArrayList<String> argumentsTypes = ((StoreStateMachineFromFunctionCallRequest) payload).getArgumentsTypes();
            try {
                contractInstancesStorageService.storeStateMachine(contractInstanceId, partyName, functionName, argumentsTypes);
                return new ChannelMessage(this.getId(), new StoreStateMachineFromFunctionCallResponse());
            } catch (IOException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            } catch (ContractInstanceNotFoundException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            }
        } else if (payload instanceof StoreStateMachineFromObligationCallRequest) {
            String contractInstanceId = ((StoreStateMachineFromObligationCallRequest) payload).getContractInstanceId();
            String obligationFunctionName = ((StoreStateMachineFromObligationCallRequest) payload).getObligationFunctionName();
            try {
                contractInstancesStorageService.storeStateMachine(contractInstanceId, obligationFunctionName);
                return new ChannelMessage(this.getId(), new StoreStateMachineFromObligationCallResponse());
            } catch (IOException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            } catch (ContractInstanceNotFoundException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            }
        } else if (payload instanceof GetFundRequest) {
            String address = ((GetFundRequest) payload).getAddress();
            String ownershipId = ((GetFundRequest) payload).getOwnershipId();
            try {
                Ownership fund = ownershipsStorageService.getFund(address, ownershipId);
                return new ChannelMessage(this.getId(), new GetFundResponse(fund));
            } catch (IOException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            } catch (OwnershipsNotFoundException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            } catch (OwnershipNotFoundException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            }
        } else if (payload instanceof GetFundsRequest) {
            String address = ((GetFundsRequest) payload).getAddress();
            try {
                ArrayList<Ownership> funds = ownershipsStorageService.getFunds(address);
                return new ChannelMessage(this.getId(), new GetFundsResponse(funds));
            } catch (IOException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            } catch (OwnershipsNotFoundException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            }
        } else if (payload instanceof AddFundsRequest) {
            HashMap<String, SingleUseSeal> funds = ((AddFundsRequest) payload).getFunds();
            try {
                ownershipsStorageService.addFunds(funds);
                return new ChannelMessage(this.getId(), new AddFundsResponse());
            } catch (IOException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            }
        } else if (payload instanceof MakeOwnershipSpentRequest) {
            String address = ((MakeOwnershipSpentRequest) payload).getAddress();
            String ownershipId = ((MakeOwnershipSpentRequest) payload).getOwnershipId();
            String contractInstanceId = ((MakeOwnershipSpentRequest) payload).getContractInstanceId();
            String unlockScript = ((MakeOwnershipSpentRequest) payload).getUnlockScript();
            try {
                ownershipsStorageService.makeOwnershipSpent(address, ownershipId, contractInstanceId, unlockScript);
                return new ChannelMessage(this.getId(), new MakeOwnershipSpentResponse());
            } catch (IOException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            } catch (OwnershipsNotFoundException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            } catch (OwnershipNotFoundException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            }
        } else {
            return new ChannelMessage(this.getId(), new RequestNotFound(message.getPayload().getClass().getSimpleName()));
        }
    }

    public void seed() {
        try {
            assetsStorageService.seed();
            ownershipsStorageService.seed();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
