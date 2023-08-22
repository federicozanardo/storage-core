package storage.models.dto.contractinstance.storestatemachine.obligation;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;

@AllArgsConstructor
@Getter
@ToString
public class StoreStateMachineFromObligationCallRequest extends ChannelMessagePayload {
    private final String contractInstanceId;
    private final String obligationFunctionName;
}
