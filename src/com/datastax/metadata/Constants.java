package com.datastax.metadata;

/**
 * Class for debugging binary formats
 */
public class Constants {
    public static boolean DEBUG = true;

    // MAGIC
    public static int ROW_MAGIC = 0x524f57;        // ROW
    public static int ROW_END = 0x50454e44;        // PEND

    public static int FILE_END = 0x454e44;         // END

}
