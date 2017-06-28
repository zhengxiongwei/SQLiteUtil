package com.test.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SQLiteUtil {
	private static Log logger = LogFactory.getLog(SQLiteUtil.class);	
	private static Statement stmt;
	private static Connection connect;

	static {
		try {
			// 获取SQLite数据库文件地址
			File targetResource = new File("test.db");
			if(!targetResource.exists()){
				FileOutputStream fous = new FileOutputStream(targetResource);
				fous.close();
				logger.debug("sqlite的DB路径为："+ targetResource.getPath());
			}
			// 获取SQLite数据库链接
			Class.forName("org.sqlite.JDBC");
			logger.debug("sqlite的DB路径为："+ targetResource.getPath());
			connect = DriverManager.getConnection("jdbc:sqlite:"+ targetResource.getPath());
		} catch (Exception e) {
			logger.error("SQLite数据库文件未找到！", e);
		}
	}

	public static void createTable(String createTableSql, String tableName) {
		try {
			if (!tableIsExist(tableName)) {
				connect.setAutoCommit(false);
				stmt = connect.createStatement();
				stmt.executeUpdate(createTableSql);
				connect.commit();
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	public static boolean tableIsExist(String tableName) {
		boolean isexist = false;
		String sql = "SELECT name FROM sqlite_master WHERE type = 'table' and tbl_name ='"
				+ tableName + "'";
		try {
			connect.setAutoCommit(false);
			stmt = connect.createStatement();
			connect.commit();
			ResultSet result = stmt.executeQuery(sql);
			while (result.next()) {	
				if (StringUtils.isNotBlank(result.getString("name"))) {
					isexist = true;
				}
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		return isexist;
	}

	public static boolean save(String saveSql) {
		boolean isSaved = false;
		try {
			connect.setAutoCommit(false);
			stmt = connect.createStatement();
			int count = stmt.executeUpdate(saveSql);
			if (count > 0) {
				isSaved = true;
				System.out.println("insert data success");
			} else {
				isSaved = false;
				System.out.println("insert data fail");
			}
			connect.commit();
			stmt.close();
			connect.close();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		return isSaved;
	}

	public static boolean delete(String delSql) {
		boolean isDeleted = false;
		try {
			connect.setAutoCommit(false);
			stmt = connect.createStatement();
			int count = stmt.executeUpdate(delSql);
			if (count > 0) {
				isDeleted = true;
				System.out.println("delete data success");
			} else {
				isDeleted = false;
				System.out.println("delete data fail");
			}
			connect.commit();
			stmt.close();
			connect.close();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		return isDeleted;
	}

	public boolean update(String updateSql) {
		boolean isUpdated = false;
		try {
			connect.setAutoCommit(false);
			stmt = connect.createStatement();
			int count = stmt.executeUpdate(updateSql);
			if (count > 0) {
				isUpdated = true;
				System.out.println("update data success");
			} else {
				isUpdated = false;
				System.out.println("update data fail");
			}
			connect.commit();
			stmt.close();
			connect.close();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		return isUpdated;
	}

	public static Object getObject(String querySql) {
		Object object = null;
		try {
			connect.setAutoCommit(false);
			stmt = connect.createStatement();
			ResultSet result = stmt.executeQuery(querySql);
			while (result.next()) {
				object = result.getObject("data");
			}
			connect.commit();
			result.close();
			stmt.close();
			connect.close();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		return object;
	}

	public static void deleteObject(String tableName,String id) {
		String delSql = "DELETE FROM "+tableName+" WHERE id='" + id + "'";
		try {
			connect.setAutoCommit(false);
			stmt = connect.createStatement();
			stmt.executeUpdate(delSql);
			connect.commit();
			stmt.close();
			connect.close();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}
	
	public static void saveObject(String tableName,String id, Object object) {
		
		try {
			if(! SQLiteUtil.tableIsExist(tableName)){
				String sqlString="CREATE TABLE "+tableName+"(id VARCHAR ,data BLOB)";
				SQLiteUtil.createTable(sqlString,tableName);
			}
			String insertSql = "INSERT INTO "+tableName+" (id, data) VALUES (?,?)";
			ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(arrayOutputStream);
			objectOutputStream.writeObject(object);
			objectOutputStream.flush();
			byte data[] = arrayOutputStream.toByteArray();
			objectOutputStream.close();
			arrayOutputStream.close();
			PreparedStatement prestmt = connect.prepareStatement(insertSql);
			prestmt.setString(1, id);
			prestmt.setBytes(2, data);
			prestmt.execute();
			connect.commit();
			connect.close();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}

	}

	public static List<Object> getObjectList(String tableName) {
		List<Object> list = null;
		try {
			if(tableIsExist(tableName)){
				String sql = "SELECT data FROM "+tableName;
				connect.setAutoCommit(false);
				stmt = connect.createStatement();
				connect.commit();
				ResultSet result = stmt.executeQuery(sql);
				list = new ArrayList<Object>();
				while (result.next()) {				
					byte[] object = result.getBytes(1);
					ByteArrayInputStream bytesIns = new ByteArrayInputStream(object);
					ObjectInputStream obIns= new ObjectInputStream(bytesIns);
					Object readObject = obIns.readObject();
					list.add(readObject);
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return list;
	}
}
