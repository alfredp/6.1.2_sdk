package com.unisys.portlet;

import com.liferay.counter.service.CounterLocalServiceUtil;
import com.liferay.portal.kernel.dao.db.DB;
import com.liferay.portal.kernel.dao.db.DBFactoryUtil;
import com.liferay.portal.kernel.dao.jdbc.DataAccess;
import com.liferay.portal.kernel.dao.jdbc.DataSourceFactoryUtil;
import com.liferay.portal.kernel.dao.orm.DynamicQuery;
import com.liferay.portal.kernel.dao.orm.DynamicQueryFactoryUtil;
import com.liferay.portal.kernel.dao.orm.RestrictionsFactoryUtil;
import com.liferay.portal.kernel.exception.SystemException;
import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.upgrade.dao.orm.UpgradeOptimizedConnectionHandler;
import com.liferay.portal.kernel.util.GetterUtil;
import com.liferay.portal.kernel.util.PropsUtil;
import com.liferay.portal.kernel.util.ProxyUtil;
import com.liferay.portal.kernel.util.StringBundler;
import com.liferay.portal.kernel.util.Validator;
import com.liferay.portal.kernel.uuid.PortalUUIDUtil;
import com.liferay.portal.model.ServiceComponent;
import com.liferay.portal.service.ServiceComponentLocalServiceUtil;
import com.liferay.portlet.asset.NoSuchCategoryException;
import com.liferay.portlet.asset.NoSuchTagException;
import com.liferay.portlet.asset.NoSuchVocabularyException;
import com.liferay.portlet.asset.service.AssetCategoryLocalServiceUtil;
import com.liferay.portlet.asset.service.AssetTagLocalServiceUtil;
import com.liferay.portlet.asset.service.AssetVocabularyLocalServiceUtil;

import javax.portlet.GenericPortlet;
import javax.portlet.PortletException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

public class CopyTagsPortlet extends GenericPortlet {

	@Override
	public void init() throws PortletException {

		try {
			DynamicQuery dynamicQuery = DynamicQueryFactoryUtil.forClass(
					ServiceComponent.class);

			dynamicQuery.add(
				RestrictionsFactoryUtil.eq(
					"buildNamespace", "CopyPortletOneTimeOnly"));

			List<ServiceComponent> serviceComponentList =
				ServiceComponentLocalServiceUtil.dynamicQuery(dynamicQuery);

			if(serviceComponentList != null &&
				serviceComponentList.size() > 0) {
				_log.warn("Copy tags has already executed");
				return;
			}
		} catch (SystemException e) {
			throw new PortletException(e);
		}

		try {
			_startDate = GetterUtil.get(
					PropsUtil.get("copy-tags-portlet.startdate"), "");

			if(Validator.isNull(_startDate)) {
				throw new PortletException("Missing start date!");
			}

			_dataSource_5_2 = getDataSource_5_2();

			_log.warn("Querying for 5.2 Tags older than " + _startDate);

			updateAssetCategories();
			updateAssetTags();
			rebuildTree();

			_log.warn("Copying Tags completed");

		} catch (Exception e) {
			throw new PortletException(e);
		}

		addServiceComponent();
	}

	@Override
	public void destroy() {
		try {
			DataSourceFactoryUtil.destroyDataSource(_dataSource_5_2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void updateAssetCategories() throws Exception {

		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			con = getConnection_5_2();

			ps = con.prepareStatement(
					"SELECT * FROM TagsVocabulary WHERE folksonomy = ? " +
							" AND modifiedDate > DATE('" + _startDate + "')");

			ps.setBoolean(1, false);

			rs = ps.executeQuery();

			while (rs.next()) {
				long vocabularyId = rs.getLong("vocabularyId");
				long groupId = rs.getLong("groupId");
				long companyId = rs.getLong("companyId");
				long userId = rs.getLong("userId");
				String userName = rs.getString("userName");
				Timestamp createDate = rs.getTimestamp("createDate");
				Timestamp modifiedDate = rs.getTimestamp("modifiedDate");
				String name = rs.getString("name");
				String description = rs.getString("description");


				_log.warn(
					"Found 5.2 Vocabulary \"" + name + "\" " + vocabularyId);

				try {
					AssetVocabularyLocalServiceUtil.getAssetVocabulary(
							vocabularyId);

					_log.warn("Skipping " + vocabularyId);
				}
				catch (NoSuchVocabularyException e) {
					addVocabulary(
						vocabularyId, groupId, companyId, userId, userName,
						createDate, modifiedDate, name, description);
				}

				copyEntriesToCategories(vocabularyId);
			}
		}
		finally {
			DataAccess.cleanUp(con, ps, rs);
		}

	}

	private void addVocabulary(
			long vocabularyId, long groupId, long companyId,
			long userId, String userName, Timestamp createDate,
			Timestamp modifiedDate, String name, String description)
		throws Exception {

		Connection con = null;
		PreparedStatement ps = null;

		try {
			con = DataAccess.getUpgradeOptimizedConnection();

			StringBundler sb = new StringBundler(4);

			sb.append("insert into AssetVocabulary (uuid_, vocabularyId, ");
			sb.append("groupId, companyId, userId, userName, createDate, ");
			sb.append("modifiedDate, name, description) values (?, ?, ?, ?, ");
			sb.append("?, ?, ?, ?, ?, ?)");

			String sql = sb.toString();

			ps = con.prepareStatement(sql);

			ps.setString(1, PortalUUIDUtil.generate());
			ps.setLong(2, vocabularyId);
			ps.setLong(3, groupId);
			ps.setLong(4, companyId);
			ps.setLong(5, userId);
			ps.setString(6, userName);
			ps.setTimestamp(7, createDate);
			ps.setTimestamp(8, modifiedDate);
			ps.setString(9, name);
			ps.setString(10, description);

			_log.warn(
				"Inserting 6.1 Vocabulary \"" + name + "\" " + vocabularyId);

			ps.executeUpdate();
		}
		finally {
			DataAccess.cleanUp(con, ps);
		}
	}

	private void copyEntriesToCategories(long vocabularyId)
		throws Exception {

		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			con = getConnection_5_2();

			ps = con.prepareStatement(
					"SELECT * FROM TagsEntry WHERE vocabularyId = ?" +
							" AND modifiedDate > DATE('" + _startDate + "')");

			ps.setLong(1, vocabularyId);

			rs = ps.executeQuery();

			while (rs.next()) {
				long entryId = rs.getLong("entryId");
				long groupId = rs.getLong("groupId");
				long companyId = rs.getLong("companyId");
				long userId = rs.getLong("userId");
				String userName = rs.getString("userName");
				Timestamp createDate = rs.getTimestamp("createDate");
				Timestamp modifiedDate = rs.getTimestamp("modifiedDate");
				long parentCategoryId = rs.getLong("parentEntryId");
				String name = rs.getString("name");

				_log.warn(
						"Found 5.2 Category \"" + name + "\"" +
								" in Vocabulary " + vocabularyId);

				try {
					AssetCategoryLocalServiceUtil.getAssetCategory(entryId);

					_log.warn("Skipping " + entryId);
				}
				catch (NoSuchCategoryException e) {
					addCategory(
						entryId, groupId, companyId, userId, userName,
						createDate, modifiedDate, parentCategoryId, name,
						vocabularyId);

					copyProperties(
						entryId, "AssetCategoryProperty",
						"categoryPropertyId", "categoryId");
				}
			}
		}
		finally {
			DataAccess.cleanUp(con, ps, rs);
		}
	}

	private void addCategory(
			long entryId, long groupId, long companyId, long userId,
			String userName, Timestamp createDate, Timestamp modifiedDate,
			long parentCategoryId, String name, long vocabularyId)
		throws Exception{

		Connection con = null;
		PreparedStatement ps = null;

		try {
			con = DataAccess.getUpgradeOptimizedConnection();

			StringBundler sb = new StringBundler(4);

			sb.append("insert into AssetCategory (uuid_, categoryId, ");
			sb.append("groupId, companyId, userId, userName, createDate, ");
			sb.append("modifiedDate, parentCategoryId, name, vocabularyId) ");
			sb.append("values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

			String sql = sb.toString();

			ps = con.prepareStatement(sql);

			ps.setString(1, PortalUUIDUtil.generate());
			ps.setLong(2, entryId);
			ps.setLong(3, groupId);
			ps.setLong(4, companyId);
			ps.setLong(5, userId);
			ps.setString(6, userName);
			ps.setTimestamp(7, createDate);
			ps.setTimestamp(8, modifiedDate);
			ps.setLong(9, parentCategoryId);
			ps.setString(10, name);
			ps.setLong(11, vocabularyId);

			_log.warn(
					"Adding 6.1 Category \"" + name +
							"\" into Vocabulary " + vocabularyId);

			ps.executeUpdate();
		}
		finally {
			DataAccess.cleanUp(con, ps);
		}
	}

	private void copyProperties(
			long categoryId, String tableName,
			String pkName, String assocationPKName)
		throws Exception {

		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			con = getConnection_5_2();

			ps = con.prepareStatement(
					"SELECT * FROM TagsProperty WHERE entryId = ? " +
					" AND modifiedDate > DATE('" +_startDate + "')");

			ps.setLong(1, categoryId);

			rs = ps.executeQuery();

			while (rs.next()) {
				long propertyId = rs.getLong("propertyId");
				long companyId = rs.getLong("companyId");
				long userId = rs.getLong("userId");
				String userName = rs.getString("userName");
				Timestamp createDate = rs.getTimestamp("createDate");
				Timestamp modifiedDate = rs.getTimestamp("modifiedDate");
				String key = rs.getString("key_");
				String value = rs.getString("value");

				addProperty(
					tableName, pkName, assocationPKName, propertyId, companyId,
					userId, userName, createDate, modifiedDate, categoryId, key,
					value);
			}
		}
		finally {
			DataAccess.cleanUp(con, ps, rs);
		}
	}

	private void addProperty(
			String tableName, String pkName, String assocationPKName,
			long propertyId, long companyId, long userId, String userName,
			Timestamp createDate, Timestamp modifiedDate, long categoryId,
			String key, String value) throws Exception {

		Connection con = null;
		PreparedStatement ps = null;

		try {
			con = DataAccess.getUpgradeOptimizedConnection();

			StringBundler sb = new StringBundler(7);

			sb.append("insert into ");
			sb.append(tableName);
			sb.append(" (");
			sb.append(pkName);
			sb.append(", companyId, userId, userName, createDate, ");
			sb.append("modifiedDate, ");
			sb.append(assocationPKName);
			sb.append(", key_, value) values (?, ?, ?, ");
			sb.append("?, ?, ?, ?, ?, ?)");

			String sql = sb.toString();

			ps = con.prepareStatement(sql);

			ps.setLong(1, propertyId);
			ps.setLong(2, companyId);
			ps.setLong(3, userId);
			ps.setString(4, userName);
			ps.setTimestamp(5, createDate);
			ps.setTimestamp(6, modifiedDate);
			ps.setLong(7, categoryId);
			ps.setString(8, key);
			ps.setString(9, value);

			ps.executeUpdate();
		}
		finally {
			DataAccess.cleanUp(con, ps);
		}
	}

	private void updateAssetTags() throws Exception {
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			con = getConnection_5_2();

			ps = con.prepareStatement(
				"SELECT TE.* FROM TagsEntry TE INNER JOIN TagsVocabulary TV " +
						"ON TE.vocabularyId = TV.vocabularyId WHERE " +
						"TV.folksonomy = ? " +
						" AND TE.modifiedDate > DATE('" +_startDate + "')");

			ps.setBoolean(1, true);

			rs = ps.executeQuery();

			while (rs.next()) {
				long entryId = rs.getLong("entryId");
				long groupId = rs.getLong("groupId");
				long companyId = rs.getLong("companyId");
				long userId = rs.getLong("userId");
				String userName = rs.getString("userName");
				Timestamp createDate = rs.getTimestamp("createDate");
				Timestamp modifiedDate = rs.getTimestamp("modifiedDate");
				String name = rs.getString("name");

				_log.warn("Found 5.2 Tag \"" + name + "\" " + entryId);

				try {
					AssetTagLocalServiceUtil.getAssetTag(entryId);

					_log.warn("Skipping " + entryId);
				}
				catch (NoSuchTagException e){

					_log.warn("Inserting 6.1 Tag \"" + name + "\"");

					addTag(
						entryId, groupId, companyId, userId, userName,
							createDate, modifiedDate, name);

					copyProperties(
							entryId, "AssetTagProperty",
							"tagPropertyId", "tagId");
				}
			}
		}
		finally {
			DataAccess.cleanUp(con, ps, rs);
		}

		updateAssetTagsCount();

	}

	protected void updateAssetTagsCount() throws Exception {
		StringBundler sb = new StringBundler(5);

		sb.append("update AssetTag set assetCount = (select count(*) from ");
		sb.append("AssetEntry inner join AssetEntries_AssetTags on ");
		sb.append("AssetEntry.entryId = AssetEntries_AssetTags.entryId ");
		sb.append("where AssetEntry.visible = TRUE and AssetTag.tagId = ");
		sb.append("AssetEntries_AssetTags.tagId)");

		DB db = DBFactoryUtil.getDB();
		db.runSQL(sb.toString());
	}


	private void addTag(
			long entryId, long groupId, long companyId, long userId,
			String userName, Timestamp createDate, Timestamp modifiedDate,
			String name) throws SQLException {

		Connection con = null;
		PreparedStatement ps = null;

		try {
			con = DataAccess.getUpgradeOptimizedConnection();

			StringBundler sb = new StringBundler(3);

			sb.append("insert into AssetTag (tagId, groupId, companyId, ");
			sb.append("userId, userName, createDate, modifiedDate, name) ");
			sb.append("values (?, ?, ?, ?, ?, ?, ?, ?)");

			String sql = sb.toString();

			ps = con.prepareStatement(sql);

			ps.setLong(1, entryId);
			ps.setLong(2, groupId);
			ps.setLong(3, companyId);
			ps.setLong(4, userId);
			ps.setString(5, userName);
			ps.setTimestamp(6, createDate);
			ps.setTimestamp(7, modifiedDate);
			ps.setString(8, name);

			ps.executeUpdate();
		}
		finally {
			DataAccess.cleanUp(con, ps);
		}
	}

	private DataSource getDataSource_5_2() throws Exception {
		Properties properties = PropsUtil.getProperties(
				"jdbc.copy-tags-portlet.", true);

		if(properties.isEmpty()) {
			throw new Exception("Missing database settings!");
		}

		return DataSourceFactoryUtil.initDataSource(properties);
	}

	private Connection getConnection_5_2() throws Exception {

		Connection con = _dataSource_5_2.getConnection();

		DatabaseMetaData metaData = con.getMetaData();

		String productName = metaData.getDatabaseProductName();

		if (productName.equals("Microsoft SQL Server")) {
			Thread currentThread = Thread.currentThread();

			ClassLoader classLoader = currentThread.getContextClassLoader();

			return (Connection) ProxyUtil.newProxyInstance(
					classLoader, new Class[]{Connection.class},
			new UpgradeOptimizedConnectionHandler(con));
		}

		return con;
	}

	private void addServiceComponent() throws PortletException {
		try {
			ServiceComponent serviceComponent =
				ServiceComponentLocalServiceUtil.createServiceComponent(
					CounterLocalServiceUtil.increment());

			serviceComponent.setBuildNamespace("CopyPortletOneTimeOnly");
			serviceComponent.setBuildNumber(1);
			serviceComponent.setBuildDate(System.currentTimeMillis());

			ServiceComponentLocalServiceUtil.addServiceComponent(
				serviceComponent);
		} catch (Exception e) {
			throw new PortletException(e);
		}
	}

	private void rebuildTree() throws Exception {
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			con = DataAccess.getUpgradeOptimizedConnection();

			ps = con.prepareStatement(
				"select distinct groupId from AssetCategory where " +
					"(leftCategoryId is null) or (rightCategoryId is null)");

			rs = ps.executeQuery();

			_log.warn("Rebuilding AssetCategory tree");

			while (rs.next()) {
				long groupId = rs.getLong("groupId");

				AssetCategoryLocalServiceUtil.rebuildTree(groupId, true);
			}
		}
		finally {
			DataAccess.cleanUp(con, ps, rs);
		}
	}

	private Log _log = LogFactoryUtil.getLog(CopyTagsPortlet.class);
	private String _startDate;
	private DataSource _dataSource_5_2;
}
