package constants;

import java.io.File;

public enum Constants {
    ASSETS_PATH {
        public String toString() {
            File currentDirectory = new File(new File(".").getAbsolutePath());
            return currentDirectory + "/service/assets/";
        }
    },
    CONTRACTS_PATH {
        public String toString() {
            File currentDirectory = new File(new File(".").getAbsolutePath());
            return currentDirectory + "/service/contracts/";
        }
    },
    CONTRACT_INSTANCES_PATH {
        public String toString() {
            File currentDirectory = new File(new File(".").getAbsolutePath());
            return currentDirectory + "/service/contract-instances/";
        }
    },
    OWNERSHIPS_PATH {
        public String toString() {
            File currentDirectory = new File(new File(".").getAbsolutePath());
            return currentDirectory + "/service/ownerships/";
        }
    }
}
