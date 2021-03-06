package org.generallib.database.mysql;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import javax.naming.NamingException;

import org.generallib.database.Database;

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

public class DatabaseMysql<T> extends Database<T> {
	private final Type type;
	
	private String dbName;
	private String tablename;
	
	private final String KEY = "dbkey";
	private final String VALUE = "dbval";

	private final MysqlConnectionPoolDataSource ds;
	private final MiniConnectionPoolManager pool;
	
	public DatabaseMysql(String address, String dbName, String tablename, String userName, String password, Type type) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, NamingException {
		this.type = type;
		
		this.dbName = dbName;
		this.tablename = tablename;
		
		ds = new MysqlConnectionPoolDataSource();
		ds.setURL("jdbc:mysql://" + address + "/" + dbName);
		ds.setUser(userName);
		ds.setPassword(password);
		ds.setCharacterEncoding("UTF-8");
		ds.setUseUnicode(true);
		ds.setAutoReconnectForPools(true);
		
		pool = new MiniConnectionPoolManager(ds, 4);
		
		Connection conn = createConnection();
		initTable(conn);
		conn.close();
	}
	
	private final String CREATEDATABASEQUARY = ""
			+ "CREATE DATABASE IF NOT EXISTS %s";
	private Connection createConnection() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		//Class.forName("com.mysql.jdbc.Driver").newInstance();
		//Connection conn = DriverManager.getConnection("jdbc:mysql://" + address, userName, password);
		Connection conn = pool.getConnection();
		
/*		PreparedStatement pstmt = conn.prepareStatement(String.format(CREATEDATABASEQUARY, dbName));
		pstmt.executeUpdate();
		pstmt.close();
		
		pstmt = conn.prepareStatement("USE "+dbName);
		pstmt.executeUpdate();
		pstmt.close();*/
		
		return conn;
	}
	
	private final String CREATETABLEQUARY = ""
			+ "CREATE TABLE IF NOT EXISTS %s ("
			+ ""+KEY+" CHAR(128) PRIMARY KEY,"
			+ ""+VALUE+" MEDIUMBLOB"
			+ ")";
	private void initTable(Connection conn) throws SQLException{
		PreparedStatement pstmt = conn.prepareStatement(String.format(CREATETABLEQUARY, tablename));
		pstmt.executeUpdate();
		pstmt.close();
	}
	
	private final String SELECTKEY = ""
			+ "SELECT "+VALUE+" FROM %s WHERE "+KEY+" = ?";
	@Override
	public T load(String key, T def) {
		Connection conn = null;
		T result = def;
		
		try {
			conn = createConnection();
			
			PreparedStatement pstmt = conn.prepareStatement(String.format(SELECTKEY, tablename));
			pstmt.setString(1, key);
			ResultSet rs = pstmt.executeQuery();			
			if(rs.next()){
				InputStream input = rs.getBinaryStream(VALUE);
				InputStreamReader isr = new InputStreamReader(input, StandardCharsets.UTF_8);
				BufferedReader br = new BufferedReader(isr);
				
				String ser = br.readLine();
				result = (T) deserialize(ser, type);
			}
			pstmt.close();
			
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException | IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(conn != null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return result;
	}

	private final String UPDATEQUARY = "INSERT INTO %s VALUES ("
			+ "?,"
			+ "?"
			+ ") "
			+ "ON DUPLICATE KEY UPDATE "
			+ ""+VALUE+" = VALUES("+VALUE+")";
	private final String DELETEQUARY = "DELETE FROM %s WHERE "+KEY+" = ?";
	@Override
	public synchronized void save(String key, T value) {
		Connection conn = null;
		try {
			conn = createConnection();
			
			if(value != null){
				String ser = serialize(value, type);
				InputStream input = new ByteArrayInputStream(ser.getBytes(StandardCharsets.UTF_8));
				
				PreparedStatement pstmt = conn.prepareStatement(String.format(UPDATEQUARY, tablename));
				pstmt.setString(1, key);
				pstmt.setBinaryStream(2, input);
				pstmt.executeUpdate();
				pstmt.close();
			}else{
				PreparedStatement pstmt = conn.prepareStatement(String.format(DELETEQUARY, tablename));
				pstmt.setString(1, key);
				pstmt.executeUpdate();
				pstmt.close();
			}
			
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(conn != null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private final String SELECTKEYS = "" + "SELECT " + KEY + " FROM %s";
	@Override
	public Set<String> getKeys() {
		Set<String> keys = new HashSet<String>();
		
		Connection conn = null;
		try {
			conn = createConnection();
			
			PreparedStatement pstmt = conn.prepareStatement(String.format(SELECTKEYS, tablename));
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()){
				keys.add(rs.getString(KEY));
			}
			rs.close();
			pstmt.close();
			
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(conn != null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return keys;
	}
	
	private final String SELECTKEYSWHERE = "" + "SELECT " + KEY + " FROM %s WHERE "+KEY+" = ?";
	@Override
	public boolean has(String key) {
		boolean result = false;
		
		Connection conn = null;
		try {
			conn = createConnection();
			
			PreparedStatement pstmt = conn.prepareStatement(String.format(SELECTKEYSWHERE, tablename));
			pstmt.setString(1, key);
			ResultSet rs = pstmt.executeQuery();
			
			result = rs.next();
			
			rs.close();
			pstmt.close();
			return result;
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	@Override
	protected void finalize() throws Throwable {
		if(pool != null)
			pool.dispose();
		super.finalize();
	}

	
}
