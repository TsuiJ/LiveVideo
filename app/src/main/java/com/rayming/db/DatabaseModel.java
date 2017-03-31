package com.rayming.db;

import android.content.Context;
import android.text.TextUtils;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;

import com.rayming.db.SQLBuilder.ClauseWhere;

/**
 * Created by Jay on 2017/3/30.
 */

public class DatabaseModel {

    private Context context;

    private DatabaseHelper databaseHelper;

    public DatabaseModel(Context context) {
        this.context = context;
        databaseHelper = new DatabaseHelper(context);
    }

    public boolean addObject(JSONObject jsonObj, String tableName) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        return sqlBuilder.prepare().set(jsonObj).insert(null);
    }

    public boolean modifyObject(JSONObject jsonObj, String tableName, String key, Object value) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        return sqlBuilder.prepare().set(jsonObj).addWhere(key, value).update(null);
    }

    public boolean modifyObject(JSONObject jsonObj, String tableName, Map<String, Object> addWhereData) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        return sqlBuilder.prepare().set(jsonObj).addWhere(addWhereData).update(null);
    }

    public boolean modifyObject(JSONObject jsonObj, String tableName, ClauseWhere... addWhereData) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        return sqlBuilder.prepare().set(jsonObj).addWhere(addWhereData).update(null);
    }

    public boolean removeObject(String tableName, String key, Object value) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        return sqlBuilder.prepare().addWhere(key, value).delete(null);
    }

    public boolean removeObject(String tableName, Map<String, Object> addWhereData) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        return sqlBuilder.prepare().addWhere(addWhereData).delete(null);
    }

    public boolean removeObject(String tableName, ClauseWhere... addWhereData) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        return sqlBuilder.prepare().addWhere(addWhereData).delete(null);
    }

    public JSONObject getObject(String tableName, List fields, String key, Object value) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare().addWhere(key, value);
        return getObject(sqlBuilder, fields);
    }

    public JSONObject getObject(String tableName, List fields, ClauseWhere... addWhereData) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare().addWhere(addWhereData);
        return getObject(sqlBuilder, fields);
    }

    public JSONObject getObject(String tableName, List fields, Map<String, Object> addWhereData) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare().addWhere(addWhereData);
        return getObject(sqlBuilder, fields);
    }

    protected JSONObject getObject(SQLBuilder sqlBuilder, List fields){
        String fieldStr = null;
        if (fields != null) {
            fieldStr = TextUtils.join(",", fields);
        }
        return sqlBuilder.field(fieldStr).find(null);
    }

    public JSONArray getObjects(String tableName, List fields) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare();
        return getObjects(sqlBuilder, fields);
    }

    public JSONArray getObjects(String tableName, List fields, String orderKey, boolean desc) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare().order(orderKey, desc);
        return getObjects(sqlBuilder, fields);
    }

    public JSONArray getObjects(String tableName, List fields, boolean distinct) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare().distinct(distinct);
        return getObjects(sqlBuilder, fields);
    }

    public JSONArray getObjects(String tableName, List fields, String orderKey, boolean desc, boolean distinct) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare().distinct(distinct).order(orderKey, desc);
        return getObjects(sqlBuilder, fields);
    }

    public JSONArray getObjects(String tableName, List fields, String orderKey, boolean desc, int start, int offset) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare().order(orderKey, desc).limit(start, offset);
        return getObjects(sqlBuilder, fields);
    }

    public JSONArray getObjects(String tableName, List fields, String key, Object value) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare().addWhere(key, value);
        return getObjects(sqlBuilder, fields);
    }

    public JSONArray getObjects(String tableName, List fields, String key, Map<String, Object> addWhereData) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare().addWhere(addWhereData);
        return getObjects(sqlBuilder, fields);
    }

    public JSONArray getObjects(String tableName, List fields, String key, ClauseWhere... addWhereData) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare().addWhere(addWhereData);
        return getObjects(sqlBuilder, fields);
    }

    public JSONArray getObjects(String tableName, List fields, String key, Object value, String orderKey, boolean desc) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare().addWhere(key, value).order(orderKey, desc);
        return getObjects(sqlBuilder, fields);
    }

    public JSONArray getObjects(String tableName, List fields, String key, Map<String, Object> addWhereData, String orderKey, boolean desc) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare().addWhere(addWhereData).order(orderKey, desc);
        return getObjects(sqlBuilder, fields);
    }

    public JSONArray getObjects(String tableName, List fields, String key, ClauseWhere[] addWhereData, String orderKey, boolean desc) {
        SQLBuilder sqlBuilder = new SQLBuilder(tableName);
        sqlBuilder.prepare().addWhere(addWhereData).order(orderKey, desc);
        return getObjects(sqlBuilder, fields);
    }

    protected JSONArray getObjects(SQLBuilder sqlBuilder, List fields) {
        String fieldStr = null;
        if (fields != null) {
            fieldStr = TextUtils.join(",", fields);
        }
        return sqlBuilder.field(fieldStr).select(null);
    }
}
