package storage.models.dto.contract.savecontract;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class SaveContractResponse extends ChannelMessagePayload {
    private final String contractId;
}
