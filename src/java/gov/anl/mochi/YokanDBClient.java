/**
 * (C) The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

package gov.anl.mochi;

import site.ycsb.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class YokanDBClient extends DB {

    private long impl; /* pointer to C++ class */

    static {
        System.loadLibrary("YokanDBClient-cpp");
    }

    @Override
    public void init() throws DBException {
        this.impl = _init();
    }

    private native long _init();

    @Override
    public void cleanup() throws DBException {
        super.cleanup();
        this._cleanup(this.impl);
    }

    private native void _cleanup(long impl);

    @Override
    public Status read(final String table, final String key, final Set<String> fields,
            final Map<String, ByteIterator> result) {
        HashMap<String, byte[]> r = new HashMap<String, byte[]>();
        Status status = this._read(this.impl, table, key, fields, r);
        if(!status.isOk()) {
            return status;
        }
        for(Map.Entry<String, byte[]> entry : r.entrySet()) {
            result.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue()));
        }
        return status;
    }

    private native Status _read(long impl, final String table, final String key, final Set<String> fields,
            final Map<String, byte[]> result);

    @Override
    public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
            final Vector<HashMap<String, ByteIterator>> result) {
        Vector<HashMap<String, byte[]>> r = new Vector<HashMap<String, byte[]>>();
        Status status = this._scan(this.impl, table, startkey, recordcount, fields, r);
        if(!status.isOk()) {
            return status;
        }
        for(Integer i = 0; i < r.size(); i++) {
            HashMap<String, ByteIterator> current_hash_map = new HashMap<String, ByteIterator>();
            for(Map.Entry<String, byte[]> entry : r.get(i).entrySet()) {
                current_hash_map.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue()));
            }
            result.add(current_hash_map);
        }
        return status;
    }

    private native Status _scan(long impl, final String table, final String startkey, final int recordcount, final Set<String> fields,
            final Vector<HashMap<String, byte[]>> result);

    @Override
    public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
        HashMap<String, byte[]> fields = new HashMap<String, byte[]>();
        for(Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            fields.put(entry.getKey(), entry.getValue().toArray());
        }
        return this._update(this.impl, table, key, fields);
    }

    private native Status _update(long impl, final String table, final String key, final Map<String, byte[]> values);

    @Override
    public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
        HashMap<String, byte[]> fields = new HashMap<String, byte[]>();
        for(Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            fields.put(entry.getKey(), entry.getValue().toArray());
        }
        return this._insert(this.impl, table, key, fields);
    }

    private native Status _insert(long impl, final String table, final String key, final Map<String, byte[]> values);

    @Override
    public Status delete(final String table, final String key) {
        return this._delete(this.impl, table, key);
    }

    private native Status _delete(long impl, final String table, final String key);
}
