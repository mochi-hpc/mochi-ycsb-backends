/**
 * (C) The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

package gov.anl.mochi;

import site.ycsb.*;
import site.ycsb.Status;

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
        return this._read(this.impl, table, key, fields, result);
    }

    private native Status _read(long impl, final String table, final String key, final Set<String> fields,
            final Map<String, ByteIterator> result);

    @Override
    public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
            final Vector<HashMap<String, ByteIterator>> result) {
        return this._scan(this.impl, table, startkey, recordcount, fields, result);
    }

    private native Status _scan(long impl, final String table, final String startkey, final int recordcount, final Set<String> fields,
            final Vector<HashMap<String, ByteIterator>> result);

    @Override
    public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
        return this._update(this.impl, table, key, values);
    }

    private native Status _update(long impl, final String table, final String key, final Map<String, ByteIterator> values);

    @Override
    public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
        return this._insert(this.impl, table, key, values);
    }

    private native Status _insert(long impl, final String table, final String key, final Map<String, ByteIterator> values);

    @Override
    public Status delete(final String table, final String key) {
        return this._delete(this.impl, table, key);
    }

    private native Status _delete(long impl, final String table, final String key);
}
