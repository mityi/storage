package io.storage.arrow.t;

import io.netty.util.internal.ThreadLocalRandom;


public class Address {
    private static final String[] STREETS = new String[]{
            "Halloway",
            "Sunset Boulvard",
            "Wall Street",
            "Secret Passageway"
    };
    private static final String[] CITIES = new String[]{
            "Brussels",
            "Paris",
            "London",
            "Amsterdam"
    };

    private final String street;
    private final int streetNumber;
    private final String city;
    private final int postalCode;

    static Address randomAddress() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return new Address(
                Util.pickRandom(STREETS),
                random.nextInt(1, 3000),
                Util.pickRandom(CITIES),
                random.nextInt(1000, 10000)
        );
    }

    public Address(String street, int streetNumber, String city, int postalCode) {
        this.street = street;
        this.streetNumber = streetNumber;
        this.city = city;
        this.postalCode = postalCode;
    }

    public String getStreet() {
        return street;
    }

    public int getStreetNumber() {
        return streetNumber;
    }

    public String getCity() {
        return city;
    }

    public int getPostalCode() {
        return postalCode;
    }
}
