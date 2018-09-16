package com.ibm.streamsx.elasticsearch.i18n;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * fetch translated strings from the correct properties file
 * according to the current locale 
 */
public class Messages
{
	// the messages* files are searched in the directory of this class
	private static final String BUNDLE_NAME = "com.ibm.streamsx.elasticsearch.i18n"; //$NON-NLS-1$

	// load the bundle based on the current locale
	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME);

	private Messages()
	{
	}

	// fetch a string from the bundle
	public static String getString(String key)
	{
		try
		{
			return RESOURCE_BUNDLE.getString(key);
		} catch (MissingResourceException e)
		{
			return '!' + key + '!';
		}
	}
}
