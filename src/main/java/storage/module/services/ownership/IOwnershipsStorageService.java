package storage.module.services.ownership;

import lcp.lib.models.ownership.Ownership;
import lcp.lib.models.singleuseseal.SingleUseSeal;
import storage.exceptions.OwnershipNotFoundException;
import storage.exceptions.OwnershipsNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public interface IOwnershipsStorageService {
    ArrayList<Ownership> getFunds(String address) throws IOException, OwnershipsNotFoundException;

    Ownership getFund(String address, String ownershipId)
            throws IOException,
            OwnershipsNotFoundException,
            OwnershipNotFoundException;

    void addFunds(HashMap<String, SingleUseSeal> funds) throws IOException;

    void makeOwnershipSpent(
            String address,
            String ownershipId,
            String contractInstanceId,
            String unlockScript
    ) throws IOException, OwnershipsNotFoundException, OwnershipNotFoundException;
}
