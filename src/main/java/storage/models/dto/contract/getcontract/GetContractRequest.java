package storage.models.dto.contract.getcontract;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class GetContractRequest extends ChannelMessagePayload {
    private final String contractId;
}
