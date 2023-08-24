package storage.module.services.asset;

import lcp.lib.models.assets.Asset;
import lcp.lib.models.assets.FungibleAsset;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import storage.constants.Constants;
import storage.core.lib.exceptions.database.DatabaseException;
import storage.core.lib.exceptions.services.asset.AssetNotFoundException;
import storage.core.lib.module.services.IAssetsStorageService;
import storage.exceptions.RocksDBDatabaseException;
import storage.module.services.StorageSerializer;
import storage.utils.RocksDBUtils;

import static org.rocksdb.util.ByteUtil.bytes;

public class AssetsStorageService extends StorageSerializer<Asset> implements IAssetsStorageService {
    private RocksDB db;

    public AssetsStorageService() {
        RocksDB.loadLibrary();
    }

    public void seed() throws RocksDBDatabaseException {
        String assetId = "stipula_coin_asd345";
        String aliceAssetId = "stipula_assetA_ed8i9wk";
        String bobAssetId = "stipula_assetB_pl1n5cc";

        FungibleAsset stipulaCoinConfig = new FungibleAsset("StipulaCoin", "STC", 10000, 2);
        FungibleAsset assetAConfig = new FungibleAsset("Asset A", "ASA", 10000, 2);
        FungibleAsset assetBConfig = new FungibleAsset("Asset B", "ASB", 10000, 2);

        Asset stipulaCoin = new Asset(assetId, stipulaCoinConfig);
        Asset assetA = new Asset(aliceAssetId, assetAConfig);
        Asset assetB = new Asset(bobAssetId, assetBConfig);

        db = RocksDBUtils.open(String.valueOf(Constants.ASSETS_PATH));
        try {
            db.put(bytes(assetId), this.serialize(stipulaCoin));
            db.put(bytes(aliceAssetId), this.serialize(assetA));
            db.put(bytes(bobAssetId), this.serialize(assetB));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            db.close();
        }

        /*System.out.println("seed: assetId => " + assetId);
        System.out.println("seed: aliceAssetId => " + aliceAssetId);
        System.out.println("seed: bobAssetId => " + bobAssetId);*/
    }

    public Asset getAssetInfo(String assetId) throws AssetNotFoundException, DatabaseException {
        db = RocksDBUtils.open(String.valueOf(Constants.ASSETS_PATH));

        Asset asset;
        try {
            asset = this.deserialize(db.get(bytes(assetId)));
        } catch (RocksDBException e) {
            throw new RocksDBDatabaseException("Error while reading from database", e);
        } finally {
            db.close();
        }

        if (asset == null) {
            throw new AssetNotFoundException(assetId);
        }

        return asset;
    }
}
