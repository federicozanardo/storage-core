package storage.models.dto.contractinstance.getcontractinstance;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class GetContractInstanceRequest extends ChannelMessagePayload {
    private final String contractInstanceId;
}
