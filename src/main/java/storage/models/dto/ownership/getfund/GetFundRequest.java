package storage.models.dto.ownership.getfund;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class GetFundRequest extends ChannelMessagePayload {
    private final String address;
    private final String ownershipId;
}
