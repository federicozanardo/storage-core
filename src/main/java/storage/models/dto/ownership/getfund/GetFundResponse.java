package storage.models.dto.ownership.getfund;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lcp.lib.models.ownership.Ownership;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class GetFundResponse extends ChannelMessagePayload {
    private final Ownership fund;
}
