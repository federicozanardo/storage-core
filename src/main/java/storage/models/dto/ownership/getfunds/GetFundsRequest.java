package storage.models.dto.ownership.getfunds;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class GetFundsRequest extends ChannelMessagePayload {
    private final String address;
}
