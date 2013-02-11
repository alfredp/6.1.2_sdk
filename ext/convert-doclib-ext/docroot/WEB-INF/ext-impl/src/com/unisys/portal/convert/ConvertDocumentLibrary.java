package com.unisys.portal.convert;

import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.util.CharPool;
import com.liferay.portal.kernel.util.FileUtil;
import com.liferay.portal.kernel.util.StringBundler;
import com.liferay.portal.kernel.util.StringPool;
import com.liferay.portal.kernel.util.StringUtil;
import com.liferay.portlet.documentlibrary.NoSuchFileException;
import com.liferay.portlet.documentlibrary.model.DLFileEntry;
import com.liferay.portlet.documentlibrary.model.DLFolder;
import com.liferay.portlet.documentlibrary.service.DLFileEntryLocalServiceUtil;
import com.liferay.portlet.documentlibrary.service.DLFolderLocalServiceUtil;
import com.liferay.portlet.documentlibrary.store.Store;
import com.liferay.portlet.documentlibrary.store.StoreFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;

public class ConvertDocumentLibrary
		extends com.liferay.portal.convert.ConvertDocumentLibrary {

	public void migrateFile(
			long companyId, long repositoryId, String fileName,
			String versionNumber) {

		Store sourceStore = StoreFactory.getInstance();

		try {
			InputStream fileAsStream = sourceStore.getFileAsStream(
					companyId, repositoryId, fileName, versionNumber);

			fileAsStream.close();
		}
		catch (NoSuchFileException e) {
			DLFileEntry fileEntryByName;
			DLFolder dlFolder;

			try {
				// repo id will always be folder id because 5.2 required folders
				 dlFolder = DLFolderLocalServiceUtil.getFolder(
						repositoryId);

				fileEntryByName =
					DLFileEntryLocalServiceUtil.getFileEntryByName(
						dlFolder.getGroupId(), dlFolder.getFolderId(),
							fileName);
			} catch (Exception e1) {
				_log.error("Error getting missing file entry info", e1);
				return;
			}

			String fileRelPath = _getFileNameVersionFile(
					companyId, repositoryId, fileName, versionNumber);

			fileRelPath = fileRelPath.replaceAll("//", "/");

			_writeLog(
					companyId + "\t" + repositoryId + "\t"
						+ fileName + "\t" + versionNumber + "\t" + fileRelPath
							+ "\t" + dlFolder.getGroupId() + "\t" +
								fileEntryByName.getUuid());

			return;
		}
		catch(Exception e) {
			// no-op
		}

		super.migrateFile(companyId, repositoryId, fileName, versionNumber);
	}

	private void _buildPath(StringBundler sb, String fileNameFragment) {
		int fileNameFragmentLength = fileNameFragment.length();

		if ((fileNameFragmentLength <= 2) || (_getDepth(sb.toString()) > 3)) {
			return;
		}

		for (int i = 0;i < fileNameFragmentLength;i += 2) {
			if ((i + 2) < fileNameFragmentLength) {
				sb.append(fileNameFragment.substring(i, i + 2));
				sb.append(StringPool.SLASH);

				if (_getDepth(sb.toString()) > 3) {
					return;
				}
			}
		}
	}

	private int _getDepth(String path) {
		String[] fragments = StringUtil.split(path, CharPool.SLASH);

		return fragments.length;
	}

	private String _getFileNameVersionFile(
		long companyId, long repositoryId, String fileName, String version) {

		String ext = StringPool.PERIOD + FileUtil.getExtension(fileName);

		if (ext.equals(StringPool.PERIOD)) {
			ext += _HOOK_EXTENSION;
		}

		int pos = fileName.lastIndexOf(CharPool.SLASH);

		if (pos == -1) {
			StringBundler sb = new StringBundler();

			String fileNameFragment = FileUtil.stripExtension(fileName);

			if (fileNameFragment.startsWith("DLFE-")) {
				fileNameFragment = fileNameFragment.substring(5);

				sb.append("DLFE" + StringPool.SLASH);
			}

			_buildPath(sb, fileNameFragment);

			return companyId + StringPool.SLASH + repositoryId +
					StringPool.SLASH + sb.toString() + StringPool.SLASH +
					fileNameFragment + ext + StringPool.SLASH +
					fileNameFragment + StringPool.UNDERLINE + version + ext;
		}
		else {
			String fileNameFragment = FileUtil.stripExtension(
					fileName.substring(pos + 1));

			return fileName + StringPool.SLASH + fileNameFragment +
				StringPool.UNDERLINE + version + ext;
		}
	}

	private void _writeLog(String s) {
		String tempDir = System.getProperty("java.io.tmpdir");

		File f = new File(tempDir + "/missing_files.txt");

		try {
			if(!f.exists()) {
				f.createNewFile();
			}

			PrintWriter printWriter = new PrintWriter(new FileWriter(f, true));
			printWriter.println(s);
			printWriter.close();
		} catch (IOException e) {
			_log.error("Error writing to temp file", e);
		}
	}

	private static final String _HOOK_EXTENSION = "afsh";

	private static Log _log = LogFactoryUtil.getLog(
			ConvertDocumentLibrary.class);

}