package storage.models.dto.ownership.getfunds;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lcp.lib.models.ownership.Ownership;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;

@AllArgsConstructor
@Getter
@ToString
public class GetFundsResponse extends ChannelMessagePayload {
    private final ArrayList<Ownership> funds;
}
