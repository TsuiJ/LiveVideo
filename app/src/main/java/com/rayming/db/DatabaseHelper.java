package com.rayming.db;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by Jay on 2017/3/30.
 */

public class DatabaseHelper extends SQLiteOpenHelper {

    private static final int DB_VERSION = 1;
    private static final String DB_NAME = "LivePlay";

    public DatabaseHelper(Context context) {
        super(context, DB_NAME, null, DB_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        String createTb = "create table `user` (`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL, `name` varchar, `age` int)";
        db.execSQL(createTb);
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int oldVersion, int newVersion) {

    }
}
