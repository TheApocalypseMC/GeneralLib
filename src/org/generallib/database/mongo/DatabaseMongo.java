package org.generallib.database.mongo;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.generallib.database.Database;
import xyz.thegamecube.cubecore.store.Storage;
import xyz.thegamecube.cubecore.store.StorageAuthentication;
import xyz.thegamecube.cubecore.store.StorageException;

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

/**
 * @author SirFaizdat
 */
public class DatabaseMongo<T> extends Database<T> {

    private Storage storage;
    private String dbName, collectionName;
    private Type type;

    public DatabaseMongo(String dbAddress, String dbName, String dbUser, String dbPass,
        String collectionName, Type type) {
        // Establish a connection to MongoDB
        String[] split = dbAddress.split(":");
        this.storage = new Storage(split[0], Integer.parseInt(split[1]),
            new StorageAuthentication().add(dbName, dbUser, dbPass).build());

        // Ensure that the collection exists (getCollection creates one if it doesn't exist)
        this.storage.getCollection(dbName, collectionName);

        this.dbName = dbName;
        this.collectionName = collectionName;
        this.type = type;
    }

    @SuppressWarnings("unchecked") @Override public T load(String key, T def) {
        try {
            Document data = storage.getData(dbName, collectionName, key);
            String json = data.getString("json");
            return (T) super.deserialize(json, type);
        } catch (StorageException e) {
            e.printStackTrace();
            return def;
        }
    }

    @Override public void save(String key, T value) {
        Document data = new Document("name", key);
        data.put("json", super.serialize(value));
        try {
            storage.insertData(dbName, collectionName, data);
        } catch (StorageException e) {
            e.printStackTrace();
        }
    }

    @Override public boolean has(String key) {
        try {
            // getData will never be null, but if the data doesn't exist then a StorageException is thrown.
            storage.getData(dbName, collectionName, key);
        } catch (StorageException e) {
            return false;
        }
        return true;
    }

    @Override public Set<String> getKeys() {
        Set<String> ret = new HashSet<>();
        FindIterable<Document> i = storage.getCollection(dbName, collectionName).find();
        i.forEach(new Block<Document>() {
            @Override public void apply(final Document document) {
                ret.add(document.getString("name"));
            }
        });
        return ret;
    }

}
