package io.storage;

import lombok.Data;

import java.util.List;

@Data
public class Collection {

    private String name;
    private List<Object> data;

    // Getters and setters
}