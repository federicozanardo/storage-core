package storage.models.dto.ownership.makeownershipspent;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class MakeOwnershipSpentRequest extends ChannelMessagePayload {
    private final String address;
    private final String ownershipId;
    private final String contractInstanceId;
    private final String unlockScript;
}
