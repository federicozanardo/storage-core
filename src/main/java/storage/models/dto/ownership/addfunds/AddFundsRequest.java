package storage.models.dto.ownership.addfunds;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lcp.lib.models.singleuseseal.SingleUseSeal;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.HashMap;

@AllArgsConstructor
@Getter
@ToString
public class AddFundsRequest extends ChannelMessagePayload {
    private final HashMap<String, SingleUseSeal> funds;
}
