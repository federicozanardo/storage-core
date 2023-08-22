package storage.models.dto.contractinstance.savecontractinstance;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lcp.lib.models.contract.ContractInstance;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class SaveContractInstanceRequest extends ChannelMessagePayload {
    private final ContractInstance contractInstance;
}
