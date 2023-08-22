package storage.models.dto.contract.getcontract;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lcp.lib.models.contract.Contract;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class GetContractResponse extends ChannelMessagePayload {
    private final Contract contract;
}
