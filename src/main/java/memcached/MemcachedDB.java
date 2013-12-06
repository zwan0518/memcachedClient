package memcached;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.BinaryCommandFactory;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.utils.AddrUtil;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.generator.CounterGenerator;

public class MemcachedDB extends AbstractClient {

	private int read_fail = 0;
	private Connection _connection;
	private MemcachedClient _client;

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
		String server = "";
		for (String s : getServers()) {
			server += s + " ";
		}

		MemcachedClientBuilder builder = new XMemcachedClientBuilder(
				AddrUtil.getAddresses(server));

		try {
			builder.setCommandFactory(new BinaryCommandFactory());
			_client = builder.build();
		} catch (IOException e) {
			System.err.println("init memcached failed!!!");
			System.err.println(e);
		}
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
					try {
						_client.add(key, getExpire(), value);
					} catch (Exception e) {
						e.printStackTrace();
					}
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
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {

		if (_client == null) {
			System.out.println("the _client is not inited !!!");
		}
		boolean flag = false;
		try {
			if (_client.get(key) != null) {
				System.out.println(_client.get(key));
				System.out.println(_client.isShutdown());
				_client.shutdown();
				System.out.println(_client.isShutdown());
				return 0;
			}
			flag = _client.add(key, getExpire(), getConstString());
		} catch (Exception e) {
			System.err.println("insert failed " + key + ":" + values);
			System.err.println(e);
		}
		if (!flag) {
			System.out.println("insert key: " + key + " failed!!!");
			return -1;
		}
		return 0;
	}

	@Override
	public int delete(String table, String key) {

		return 0;
	}

	@Override
	public void cleanup() {
		_client = null;
		try {
			if (_connection != null) {
				_connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public String get(String key) throws IOException {
		if (_client == null) {
			throw new IOException("can't connect.");
		}
		try {
			return (String) _client.get(key);
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MemcachedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) throws IOException {

		MemcachedDB mc = new MemcachedDB();
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
