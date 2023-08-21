package storage.models.dto.asset.getassetinfo;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class GetAssetInfoRequest extends ChannelMessagePayload {
    private final String assetId;
}
