package storage.module.services.asset;

import lcp.lib.models.assets.Asset;
import storage.exceptions.AssetNotFoundException;

import java.io.IOException;

public interface IAssetsStorageService {
    Asset getAssetInfo(String assetId) throws IOException, AssetNotFoundException;
}
