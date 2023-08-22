package storage.models.dto.contractinstance.storestatemachine.function;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;

@AllArgsConstructor
@Getter
@ToString
public class StoreStateMachineFromFunctionCallRequest extends ChannelMessagePayload {
    private final String contractInstanceId;
    private final String partyName;
    private final String functionName;
    private final ArrayList<String> argumentsTypes;
}
