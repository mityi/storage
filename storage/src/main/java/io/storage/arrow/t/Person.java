package io.storage.arrow.t;

import io.netty.util.internal.ThreadLocalRandom;


public class Person {
    private static final String[] FIRST_NAMES = new String[]{"John", "Jane", "Gerard", "Aubrey", "Amelia"};
    private static final String[] LAST_NAMES = new String[]{"Smith", "Parker", "Phillips", "Jones"};

    private final String firstName;
    private final String lastName;
    private final int age;
    private final Address address;

    public static Person randomPerson() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return new Person(
                Util.pickRandom(FIRST_NAMES),
                Util.pickRandom(LAST_NAMES),
                random.nextInt(0, 120),
                Address.randomAddress()
        );
    }

    public Person(String firstName, String lastName, int age, Address address) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.address = address;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public int getAge() {
        return age;
    }


    public Address getAddress() {
        return address;
    }
}
