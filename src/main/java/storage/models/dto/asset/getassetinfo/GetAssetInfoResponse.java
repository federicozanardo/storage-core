package storage.models.dto.asset.getassetinfo;

import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lcp.lib.models.assets.Asset;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class GetAssetInfoResponse extends ChannelMessagePayload {
    private final Asset assetInfo;
}
