package storage.models.dto.contractinstance.getcontractinstance;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lcp.lib.models.contract.ContractInstance;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class GetContractInstanceResponse extends ChannelMessagePayload {
    private final ContractInstance contractInstance;
}
