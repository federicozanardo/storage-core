package storage.module;

import lcp.lib.communication.module.Module;
import lcp.lib.communication.module.channel.ChannelMessage;
import lcp.lib.communication.module.channel.ChannelMessagePayload;
import lcp.lib.communication.module.channel.ModuleChannel;
import lcp.lib.communication.module.channel.responses.RequestNotFound;
import lcp.lib.models.assets.Asset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.exceptions.AssetNotFoundException;
import storage.models.dto.asset.getassetinfo.GetAssetInfoRequest;
import storage.models.dto.asset.getassetinfo.GetAssetInfoResponse;
import storage.module.services.AssetsStorageService;
import storage.module.services.ContractInstancesStorageService;
import storage.module.services.ContractsStorageService;
import storage.module.services.OwnershipsStorageService;

import java.io.IOException;

public class StorageModule extends Module {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final AssetsStorageService assetsStorageService;
    private final ContractInstancesStorageService contractInstancesStorageService;
    private final ContractsStorageService contractsStorageService;
    private final OwnershipsStorageService ownershipsStorageService;

    public StorageModule() {
        this.assetsStorageService = new AssetsStorageService();
        this.contractInstancesStorageService = new ContractInstancesStorageService();
        this.contractsStorageService = new ContractsStorageService();
        this.ownershipsStorageService = new OwnershipsStorageService();
    }

    @Override
    public void send(String receiverModuleId, ChannelMessagePayload payload) {
        logger.debug("[{}] payload: {}", new Object() {}.getClass().getEnclosingMethod().getName(), payload);
        ModuleChannel channel = this.findChannel(this.getId(), receiverModuleId);

        if (channel != null) {
            channel.send(new ChannelMessage(this.getId(), payload));
        } else {
            logger.error("Impossible to find a channel with {}!", receiverModuleId);
        }
    }

    @Override
    public void receive(ChannelMessage message) {
        logger.debug("[{}] from: {}, payload: {}", new Object() {}.getClass().getEnclosingMethod().getName(), message.getSenderModuleId(), message.getPayload());
    }

    @Override
    public ChannelMessage sendAndReceive(String receiverModuleId, ChannelMessagePayload payload) {
        logger.debug("[{}] payload: {}", new Object() {}.getClass().getEnclosingMethod().getName(), payload);
        ModuleChannel channel = this.findChannel(this.getId(), receiverModuleId);

        if (channel != null) {
            return channel.sendAndReceive(new ChannelMessage(this.getId(), payload));
        } else {
            logger.error("Impossible to find a channel with {}!", receiverModuleId);
            return null;
        }
    }

    @Override
    public ChannelMessage receiveAndResponse(ChannelMessage message) {
        logger.debug("[{}] from: {}, payload: {}", new Object() {}.getClass().getEnclosingMethod().getName(), message.getSenderModuleId(), message.getPayload());

        if (message.getPayload() instanceof GetAssetInfoRequest) {
            try {
                Asset assetInfo = assetsStorageService.getAssetInfo(((GetAssetInfoRequest) message.getPayload()).getAssetId());
                return new ChannelMessage(this.getId(), new GetAssetInfoResponse(assetInfo));
            } catch (IOException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            } catch (AssetNotFoundException e) {
                // TODO: handle it
                throw new RuntimeException(e);
            }
        } else {
            return new ChannelMessage(this.getId(), new RequestNotFound(message.getPayload().getClass().getSimpleName()));
        }
    }

    public void seed() {
        try {
            assetsStorageService.seed();
            ownershipsStorageService.seed();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
