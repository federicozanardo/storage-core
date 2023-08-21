package storage.exceptions;

public class OwnershipsNotFoundException extends Exception {
    public OwnershipsNotFoundException() {
        super();
    }

    public OwnershipsNotFoundException(String address) {
        super("There are no funds associated to the address = " + address);
    }
}
