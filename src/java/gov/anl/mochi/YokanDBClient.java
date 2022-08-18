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

    static {
        System.loadLibrary("YokabDBClient-cpp");
    }

    @Override
    public void init() throws DBException {
        // TODO
        System.out.println("YokanDBClient::init()");
    }

    private native void _init();

    @Override
    public void cleanup() throws DBException {
        super.cleanup();
        // TODO
        System.out.println("YokanDBClient::cleanup()");
    }

    private native void _cleanup();

    @Override
    public Status read(final String table, final String key, final Set<String> fields,
            final Map<String, ByteIterator> result) {
        // TODO
        System.out.println("YokanDBClient::read()");
        return Status.OK;
    }

    private native Status _read(final String table, final String key, final Set<String> fields,
            final Map<String, ByteIterator> result);

    @Override
    public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
            final Vector<HashMap<String, ByteIterator>> result) {
        // TODO
        System.out.println("YokanDBClient::scan()");
        return Status.OK;
    }

    private native Status _scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
            final Vector<HashMap<String, ByteIterator>> result);

    @Override
    public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
        // TODO
        System.out.println("YokanDBClient::update()");
        return Status.OK;
    }

    private native Status _update(final String table, final String key, final Map<String, ByteIterator> values);

    @Override
    public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
        // TODO
        System.out.println("YokanDBClient::insert()");
        return Status.OK;
    }

    private native Status _insert(final String table, final String key, final Map<String, ByteIterator> values);

    @Override
    public Status delete(final String table, final String key) {
        // TODO
        System.out.println("YokanDBClient::delete()");
        return Status.OK;
    }

    private native Status _delete(final String table, final String key);
}
