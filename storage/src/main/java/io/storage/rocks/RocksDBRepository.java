package io.storage.rocks;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Repository;
import org.springframework.util.SerializationUtils;

import java.util.Optional;

@Slf4j
@Repository
@AllArgsConstructor
public class RocksDBRepository implements KVRepository<byte[], byte[]> {
    private final RocksDB db;
    //todo use locs instead synchronized

    @Override
    public synchronized boolean save(byte[] key, byte[] value) {
        log.debug("saving value '{}' with key '{}'", value, key);
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            log.error("Error saving entry. Cause: '{}', message: '{}'", e.getCause(), e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public synchronized Optional<byte[]> find(byte[] key) {
        byte[] value = null;
        try {
            byte[] bytes = db.get(key);
            if (bytes != null) {
                value = bytes;
            }
        } catch (RocksDBException e) {
            log.error(
                    "Error retrieving the entry with key: {}, cause: {}, message: {}",
                    key,
                    e.getCause(),
                    e.getMessage()
            );
        }
        log.debug("finding key '{}' returns '{}'", key, value);
        return value != null ? Optional.of(value) : Optional.empty();
    }

    @Override
    public synchronized boolean delete(byte[] key) {
        log.debug("deleting key '{}'", key);
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            log.error("Error deleting entry, cause: '{}', message: '{}'", e.getCause(), e.getMessage());
            return false;
        }
        return true;
    }
}
