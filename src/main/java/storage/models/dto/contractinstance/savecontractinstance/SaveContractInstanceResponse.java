package storage.models.dto.contractinstance.savecontractinstance;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class SaveContractInstanceResponse extends ChannelMessagePayload {
    private final String contractInstanceId;
}
