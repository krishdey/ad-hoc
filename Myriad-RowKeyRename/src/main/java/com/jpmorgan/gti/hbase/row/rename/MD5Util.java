package com.jpmorgan.gti.hbase.row.rename;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

public class MD5Util {

	public static String getMD5(String input) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] messageDigest = md.digest(input.getBytes(StandardCharsets.UTF_8));
			return format32Way3(messageDigest);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	public static String format32Way3(byte[] messageDigest) {
		return DatatypeConverter.printHexBinary(messageDigest);
	}

	public static String getSalt(String input) {
		int hash = input.hashCode();
		return Integer.toHexString(hash);
	}

	public static String calcMD5SaltedTail(String input, int x) {
		String md5 = getMD5(input);
		if (x <= 0) {
			return md5;
		}
		int start = (input.length() - x >= 0) ? (input.length() - x) : 0;
		String tail = input.substring(start);
		return md5 + tail;
	}

	public static String calcMD5SaltedTail(String input) {
		if (input == null)
			return input;
		return calcMD5SaltedTail(input, 5);
	}
	
	public static String calcMD5SaltedTail(String leftNodeId,String linkTypeId, String rightNodeId) {
		if(leftNodeId == null || linkTypeId == null || rightNodeId == null) {
			return null;
		}
		return calcMD5SaltedTail(leftNodeId)+ linkTypeId + calcMD5SaltedTail(rightNodeId);
	}

}
