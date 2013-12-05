package memcached;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.utils.AddrUtil;

import org.json.JSONArray;
import org.json.JSONObject;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.generator.CounterGenerator;

public class MemcachedDB extends DB {
	enum Status {
		SUCCESS(0), FAIL(-1), EXCEPTION(-2);
		int status;

		private Status(int status) {
			this.status = status;
		}

		public int getValue() {
			return status;
		}
	}

	private static final boolean isload;
	private static final String[] servers;
	// private static final String db_host;
	// private static final String db_port;
	// private static final String db_name;
	// private static final String db_url_pref = "jdbc:mysql://";
	private static int read_fail = 0;

	private static final String CONST_STRING;
	private static final int EXPIRE = 32768;
	private Connection _connection;
	private MemcachedClient _client;
	static {
		try {
			Properties p = new Properties();
			System.out.println("loader" + MemcachedDB.class.getClassLoader());
			p.load(MemcachedDB.class.getClassLoader().getResourceAsStream("memcached/connect.properties"));
			isload = Boolean.parseBoolean(p.getProperty("isload"));
			String mem = p.getProperty("memcached");
			System.err.println(mem);
			JSONArray array = new JSONArray(mem);
			servers = new String[array.length()];
			for (int i = 0; i < array.length(); i++) {
				JSONObject json = new JSONObject(array.getString(i));
				String mem_host = json.getString("ip");
				String mem_port = json.getString("port");
				servers[i] = mem_host + ":" + mem_port;
			}

			CONST_STRING = "abcd";
			// for a long value test
			// char[] chs = new char[4004];
			// for (int i = 0; i < 2025; i++) {
			// chs[i] = 'Y';
			// }
			// CONST_STRING = new String(chs);
		} catch (Exception e) {
			throw new RuntimeException("Failed to init app configuration", e);
		}
	}

	@Override
	public void init() {
		initMemcache();

		if (isload) {
			initMySQL();
			loadData();
		}
	}

	private void initMySQL() {

//		try {
//			Class.forName("com.mysql.jdbc.Driver");
//			String db_url = db_url_pref + db_host + ":" + db_port + "/"
//					+ db_name;
//			_connection = DriverManager.getConnection(db_url, "root",
//					"c,123456");
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}

	}

	private void initMemcache() {
		String server = "";
		for (String s : servers) {
			server += s + " ";
		}

		MemcachedClientBuilder builder = new XMemcachedClientBuilder(
				AddrUtil.getAddresses(server));

		try {
			_client = builder.build();
		} catch (IOException e) {
			System.err.println("init memcached failed!!!");
			System.err.println(e);
		}
		// System.out.println("init success:");
		// System.out.println("server:" + server);
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
						_client.add(key, EXPIRE, value);
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
				read_fail ++;
				System.err.println("read_fail:" + read_fail);
				return Status.FAIL.getValue();
			}
			// System.out.println("k:" + key + ", v:" + v);
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
			System.out.println("the _client is not inited !!!");
		}
		// System.out.println("key: " + key);
		// System.out.println("key: " + key.length());
		// System.out.println("value: " + CONST_STRING);
		boolean flag = false;
		try {
			if(_client.get(key) != null){
				System.out.println(_client.get(key));
				return 0;
			}
			
			flag = _client.add(key, EXPIRE, CONST_STRING);
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
