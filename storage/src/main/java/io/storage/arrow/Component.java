package io.storage.arrow;

import java.util.Map;

public record Component(String key, String relatedKey, String relatedType, Map<String, String> values) {

}
