package memcached;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.generator.CounterGenerator;

public class MemcachedClient extends AbstractClient {
	private Connection _connection;
	private MemCachedClient _client;
	private int read_fail = 0;

	@Override
	public void init() {
		initMemcache();

		if (isload()) {
			initMySQL();
			loadData();
		}
	}

	private void initMySQL() {
		// try {
		// Class.forName("com.mysql.jdbc.Driver");
		// String db_url = db_url_pref + db_host + ":" + db_port + "/"
		// + db_name;
		// _connection = DriverManager.getConnection(db_url, "root",
		// "c,123456");
		// } catch (ClassNotFoundException e) {
		// e.printStackTrace();
		// } catch (SQLException e) {
		// e.printStackTrace();
		// }
	}

	private void initMemcache() {
		String[] servers = getServers();
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(servers);
		pool.initialize();
		_client = new MemCachedClient(true);
	}

	private void loadData() {

		Statement s;
		try {
			s = _connection.createStatement();
			int begain = 0;
			int end = 10000;
			int wholeNum = 0;
			String query = "select count(*) from data";
			s.execute(query);
			ResultSet r = s.getResultSet();
			r.next();
			wholeNum = r.getInt(1);
			s.close();

			for (; begain < wholeNum;) {
				System.out.println("load data for " + begain);
				query = "select * from data limit " + begain + "," + end;
				Statement s1 = _connection.createStatement();
				s1.execute(query);
				ResultSet rs = s1.getResultSet();
				while (rs.next()) {
					String key = rs.getString(1);
					String value = rs.getString(2);
					_client.add(key, value);
				}
				begain += 10000;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		if (table == null || key == null) {
			return Status.EXCEPTION.getValue();
		}
		try {
			String v = get(key);
			if (v == null) {
				System.err.println("k:" + key);
				read_fail++;
				System.err.println("read_fail:" + read_fail);
				return Status.FAIL.getValue();
			}
			return Status.SUCCESS.getValue();
		} catch (Exception e) {
			System.out.println("Error in processing read of key " + table
					+ ": " + e);
			return Status.EXCEPTION.getValue();
		}

	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		return 0;
	}

	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {

		if (_client == null) {
			System.out.println("the _client is null !!!");
			return -1;
		}
		if (_client.get(key) != null) {
			System.out.println(_client.get(key));
			return 0;
		}
		boolean flag = _client.add(key, getConstString());

		if (!flag) {
			System.out.println("insert key: " + key + " failed!!!");
			return -1;
		}
		return 0;
	}

	@Override
	public void cleanup() {
		_client = null;
		try {
			if (_connection != null) {
				_connection.close();
			}
			// System.out.println("read failed " + read_fail);
			// System.out.println("mysql and memcached connections are closed!");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public String get(String key) throws IOException {
		if (_client == null) {
			throw new IOException("can't connect.");
		}
		return (String) _client.get(key);
	}

	public static void main(String[] args) throws IOException {

		MemcachedClient mc = new MemcachedClient();
		mc.init();
		HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

		for (int i = 0; i < 1; i++) {
			String fieldkey = "field" + i;
			ByteIterator data = new RandomByteIterator(
					new CounterGenerator(0).nextInt());
			values.put(fieldkey, data);
		}
		mc.insert("a", "b", values);
	}
}
