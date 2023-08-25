package storage.module.services.asset;

import lcp.lib.models.assets.Asset;
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
