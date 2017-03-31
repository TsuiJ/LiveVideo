package com.rayming.db;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by Jay on 2017/3/30.
 */

public class SQLBuilder {

    private static Logger logger = Logger.getLogger(SQLBuilder.class.getName());

    public SQLBuilder() {

    }

    public static class ClauseWhere {

        private String key = null;
        private Object value = null;
        private WhereType type = null;

        public ClauseWhere(String key, Object value) {
            this.key = key.trim();
            this.value = value;
            this.type = WhereType.EQ;
        }

        public ClauseWhere(String key, Object value, WhereType type) {
            this.key = key.trim();
            this.value = value;
            this.type = type;
        }

        private String getInPattern() {
            StringBuffer sb = new StringBuffer();
            if (this.type == WhereType.IN && this.value instanceof List) {
                List<Object> values = (List<Object>) this.value;
                for (int i = 0, n = values.size(); i < n; i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    sb.append("?");
                }
            }
            return sb.toString();
        }

        public Object getValue() {
            return this.value;
        }

        public String getPattern() {
            switch (type) {
                case EQ:
                    return " (`" + this.key + "`=?) ";
                case IN:
                    return " (`" + this.key + "` IN (" + getInPattern() + ")) ";
                case LT:
                    return " (`" + this.key + "`<?) ";
                case GT:
                    return " (`" + this.key + "`>?) ";
                case GEQ:
                    return " (`" + this.key + "`>=?) ";
                case LEQ:
                    return " (`" + this.key + "`<=?) ";
            }
            return " (`" + this.key + "`=?) ";
        }
    }

    public enum SQLType {INSERT, SELECT, UPDATE, DELETE}

    public enum WhereType {EQ, IN, GT, LT, GEQ, LEQ}

    private List<String> mFields = new ArrayList<>();
    private List<ClauseWhere> mWheres = new ArrayList<>();
    private List<ClauseValue> mValues = new ArrayList<>();

    private String mStatementSQL = null;
    List<Object> mStatementParams = new ArrayList<>();
    private String mOrder = null;
    private boolean mDistinct = false;

    private String mTableName = null;

    private int limitStart = 0;
    private int limitOffset = 0;

    public SQLBuilder(String tableName){
        mTableName = tableName;
    }

    protected String tableName() {
        return mTableName;
    }

    public JSONObject find(Connection conn) {
        if (conn == null) {
            return null;
        }

        PreparedStatement preStatement = null;
        ResultSet resultSet = null;

        prepareStatement(SQLType.SELECT);
        try {
            preStatement = conn.prepareStatement(mStatementSQL);
            setStatementParams(preStatement);
            resultSet = preStatement.executeQuery();
            if (resultSet == null) {
                return null;
            }

            JSONObject json = new JSONObject();
            if (!resultSet.next()) {
                return json;
            }
            ResultSetMetaData metaData = resultSet.getMetaData();
            String colName = null;
            String colType = null;
            for (int i = 1, n = metaData.getColumnCount(); i <= n; i++) {
                colName = metaData.getColumnName(i);
                colType = metaData.getColumnTypeName(i).toLowerCase();
                if ("varchar".equals(colType) || "char".equals(colType)) {
                    json.put(colName, resultSet.getString(i));
                } else if (colType.startsWith("int") || colType.startsWith("tinyint")) {
                    json.put(colName, resultSet.getInt(i));
                } else {
                    json.put(colName, resultSet.getString(i));
                }
            }
            logger.info("find.data=" + json.toString());
            return json;
        } catch (SQLException e) {
            logger.warning("find.SQLException:" + e);
        } catch (Exception e) {
            logger.warning("find.Exception:" + e);
        } finally {
            try {
                if (preStatement != null) {
                    preStatement.close();
                }
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                logger.warning("find.close.SQLException:" + e);
            }
        }
        return null;
    }

    public JSONArray select(Connection conn) {
        if (conn == null) {
            return null;
        }

        PreparedStatement preStatement = null;
        ResultSet resultSet = null;

        prepareStatement(SQLType.SELECT);
        try {
            preStatement = conn.prepareStatement(mStatementSQL);
            setStatementParams(preStatement);
            resultSet = preStatement.executeQuery();
            if (resultSet == null) {
                return null;
            }
            ResultSetMetaData metaData = resultSet.getMetaData();
            String colName = null, colType = null;
            int i = 0, n = metaData.getColumnCount();
            JSONArray array = new JSONArray();
            while (resultSet.next()) {
                JSONObject json = new JSONObject();
                for (i = 1; i <= n; i++) {
                    colName = metaData.getColumnName(i);
                    colType = metaData.getColumnTypeName(i).toLowerCase();
                    if ("varchar".equals(colType) || "char".equals(colType)) {
                        json.put(colName, resultSet.getString(i));
                    } else if (colType.startsWith("int") || colType.startsWith("tinyint")) {
                        json.put(colName, resultSet.getInt(i));
                    } else {
                        json.put(colName, resultSet.getString(i));
                    }
                }
                array.put(json);
            }
            logger.info("select.data.size=" + array.length());
            return array;
        } catch (SQLException e) {
            logger.warning("select.SQLException:" + e);
        } catch (Exception e) {
            logger.warning("select.Exception:" + e);
        } finally {
            try {
                if (preStatement != null) {
                    preStatement.close();
                }
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                logger.warning("select.close.SQLException:" + e);
            }
        }
        return null;
    }

    public boolean update(Connection conn) {
        if (conn == null) {
            return false;
        }

        PreparedStatement preStatement = null;
        ResultSet resultSet = null;

        prepareStatement(SQLType.UPDATE);
        try {
            preStatement = conn.prepareStatement(mStatementSQL);
            setStatementParams(preStatement);
            int num = preStatement.executeUpdate();
            preStatement.close();
            logger.info("update.rowCount=" + num);
            return (num > 0);
        } catch (SQLException e) {
            logger.warning("update.SQLException:" + e);
            return false;
        } catch (Exception e) {
            logger.warning("update.Exception:" + e);
            return false;
        } finally {
            try {
                if (preStatement != null) {
                    preStatement.close();
                }
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                logger.warning("update.close.SQLException:" + e);
            }
        }
    }

    protected void setStatementParams(PreparedStatement preStatement) throws SQLException {
        int idx = 0;
        Object value = null;
        List<Object> values = null;
        StringBuffer tempSB = new StringBuffer();
        logger.info("mStatementParams.size=" + mStatementParams.size());
        for (int i = 0, n = mStatementParams.size(); i < n; i++) {
            value = mStatementParams.get(i);
            if (value instanceof List) {
                values = (List<Object>) value;
                for (int j = 0, m = values.size(); j < m; j++) {
                    idx = idx + 1;
                    preStatement.setObject(idx, values.get(j));
                    tempSB.append("(idx=" + idx + ",value=" + values.get(j) + ")");
                }
            } else {
                idx = idx + 1;
                preStatement.setObject(idx, value);
                tempSB.append("(idx=" + idx + ",value=" + value + ")");
            }
        }
        logger.info("PreparedStatement.setObject.values=" + tempSB.toString());
    }

    public boolean insert(Connection conn) {
        if (mValues.size() == 0) {
            return false;
        }
        if (conn == null) {
            return false;
        }

        PreparedStatement preStatement = null;

        prepareStatement(SQLType.INSERT);
        try {
            preStatement = conn.prepareStatement(mStatementSQL);
            for (int i = 0, n = mStatementParams.size(); i < n; i++) {
                preStatement.setObject(i + 1, mStatementParams.get(i));
            }
            int num = preStatement.executeUpdate();
            preStatement.close();
            logger.info("insert.rowCount=" + num);
            return (num > 0);
        } catch (SQLException e) {
            logger.warning("insert.SQLException:" + e);
            return false;
        } catch (Exception e) {
            logger.warning("insert.Exception:" + e);
            return false;
        } finally {
            try {
                if (preStatement != null) {
                    preStatement.close();
                }
            } catch (SQLException e) {
                logger.warning("insert.close.SQLException:" + e);
            }
        }
    }

    public boolean delete(Connection conn) {
        if (conn == null) {
            return false;
        }

        PreparedStatement preStatement = null;

        prepareStatement(SQLType.DELETE);
        try {
            preStatement = conn.prepareStatement(mStatementSQL);
            for (int i = 0, n = mStatementParams.size(); i < n; i++) {
                preStatement.setObject(i + 1, mStatementParams.get(i));
            }
            preStatement.executeUpdate();
            return true;
        } catch (SQLException e) {
            logger.warning("delete.SQLException:" + e);
            return false;
        } catch (Exception e) {
            logger.warning("delete.Exception:" + e);
            return false;
        } finally {
            try {
                if (preStatement != null) {
                    preStatement.close();
                }
            } catch (SQLException e) {
                logger.warning("delete.close.SQLException:" + e);
            }
        }
    }

    protected void prepareStatement(SQLType type) {
        StringBuilder sqlSB = new StringBuilder();
        if (type == SQLType.UPDATE) {
            sqlSB.append("UPDATE `" + tableName() + "` SET ");
            if (mValues.size() > 0) {
                ClauseValue clause;
                for (int i = 0, n = mValues.size(); i < n; i++) {
                    clause = mValues.get(i);
                    mStatementParams.add(clause.getValue());
                    if (i > 0) {
                        sqlSB.append(",");
                    }
                    sqlSB.append(clause.getPattern());
                }
            }
            sqlSB.append(" WHERE 1=1");
            if (mWheres.size() > 0) {
                ClauseWhere clause;
                for (int i = 0, n = mWheres.size(); i < n; i++) {
                    clause = mWheres.get(i);
                    mStatementParams.add(clause.getValue());
                    sqlSB.append(" AND ");
                    sqlSB.append(clause.getPattern());
                }
            }
        } else if (type == SQLType.SELECT) {
            sqlSB.append("SELECT ");
            if (mFields.size() > 0) {
                if (mDistinct) {
                    sqlSB.append(" DISTINCT ");
                }
                String fieldstr = mFields.toString();
                sqlSB.append(fieldstr.substring(1, fieldstr.length() - 1));
            } else {
                sqlSB.append(" * ");
            }
            sqlSB.append(" FROM `" + tableName() + "` ");
            sqlSB.append(" WHERE 1=1");
            if (mWheres.size() > 0) {
                ClauseWhere clause;
                for (int i = 0, n = mWheres.size(); i < n; i++) {
                    clause = mWheres.get(i);
                    mStatementParams.add(clause.getValue());
                    sqlSB.append(" AND ");
                    sqlSB.append(clause.getPattern());
                }
            }
            if (mOrder != null) {
                sqlSB.append(mOrder);
            }
            if (limitOffset > 0 && limitStart >= 0) {
                sqlSB.append("limit " + limitStart + "," + limitOffset);
            }
        } else if (type == SQLType.INSERT) {
            sqlSB.append("INSERT INTO `" + tableName() + "` (");
            ClauseValue clause = null;
            StringBuilder patternSB = new StringBuilder();
            for (int i = 0, n = mValues.size(); i < n; i++) {
                clause = mValues.get(i);
                mStatementParams.add(clause.getValue());
                if (i > 0) {
                    sqlSB.append(",");
                    patternSB.append(",");
                }
                patternSB.append("?");
                sqlSB.append(clause.getKey());
            }
            sqlSB.append(") VALUES (" + patternSB.toString() + ")");
        } else if (type == SQLType.DELETE) {
            sqlSB.append("Delete from " + tableName());
            sqlSB.append(" WHERE 1=1");
            if (mWheres.size() > 0) {
                ClauseWhere clause;
                for (int i = 0, n = mWheres.size(); i < n; i++) {
                    clause = mWheres.get(i);
                    mStatementParams.add(clause.getValue());
                    sqlSB.append(" AND ");
                    sqlSB.append(clause.getPattern());
                }
            }
        }
        mStatementSQL = sqlSB.toString();
        logger.info("StatementSQL=" + mStatementSQL);
    }

    public SQLBuilder set(String key, Object value) {
        mValues.add(new ClauseValue(key.trim(), value));
        return this;
    }

    public SQLBuilder setx(String key, Object value, ValueCalc calc) {
        mValues.add(new ClauseValue(key.trim(), value, calc));
        return this;
    }

    public SQLBuilder set(Map<String, Object> data) {
        for (String key : data.keySet()) {
            mValues.add(new ClauseValue(key.trim(), data.get(key)));
        }
        return this;
    }

    public SQLBuilder set(JSONObject jsonObject) {
        try {
            Iterator<String> keys = jsonObject.keys();
            while (keys.hasNext()){
                String key = keys.next();
                mValues.add(new ClauseValue(key.trim(), jsonObject.get(key)));
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return this;
    }

    public SQLBuilder addWhere(String key, Object value) {
        mWheres.add(new ClauseWhere(key.trim(), value));
        return this;
    }

    public SQLBuilder addWhere(String key, Object value, WhereType type) {
        mWheres.add(new ClauseWhere(key.trim(), value, type));
        return this;
    }

    public SQLBuilder addWhere(Map<String, Object> data) {
        for (String key : data.keySet()) {
            mWheres.add(new ClauseWhere(key.trim(), data.get(key)));
        }
        return this;
    }

    public SQLBuilder addWhere(ClauseWhere clauseWhere) {
        mWheres.add(clauseWhere);
        return this;
    }

    public SQLBuilder addWhere(ClauseWhere... clauseWheres) {
        for (int i = 0; i < clauseWheres.length; i++) {
            mWheres.add(clauseWheres[i]);
        }
        return this;
    }

    public SQLBuilder limit(int start, int offset) {
        limitStart = start;
        limitOffset = offset;
        return this;
    }

    public SQLBuilder order(String field, boolean desc) {
        mOrder = " ORDER BY `" + field.trim() + "` ";
        if (desc) {
            mOrder += " DESC ";
        }
        return this;
    }

    public SQLBuilder distinct(boolean distinct) {
        mDistinct = distinct;
        return this;
    }

    public SQLBuilder field(String str) {
        mFields.clear();
        if (str != null) {
            String[] strArr = str.split(",");
            for (int i = 0, n = strArr.length; i < n; i++) {
                mFields.add("`" + strArr[i].trim() + "`");
            }
        }
        return this;
    }


    public SQLBuilder prepare() {
        mFields.clear();
        mWheres.clear();
        mValues.clear();
        mOrder = null;
        mDistinct = false;
        mStatementSQL = null;
        mStatementParams.clear();
        limitStart = 0;
        limitOffset = 0;
        return this;
    }

    public enum ValueCalc {add, sub, mul, div}

    public class ClauseValue {

        private String key = null;
        private Object value = null;
        private ValueCalc calc = null;

        public ClauseValue(String key, Object value) {
            this.key = key.trim();
            this.value = value;
        }

        public ClauseValue(String key, Object value, ValueCalc calc) {
            this.key = key.trim();
            this.value = value;
            this.calc = calc;
        }

        public Object getValue() {
            return this.value;
        }

        public String getPattern() {
            if (calc != null) {
                switch (calc) {
                    case add:
                        return " `" + this.key + "`=(`" + this.key + "`+?) ";
                    case sub:
                        return " `" + this.key + "`=(`" + this.key + "`-?) ";
                    case mul:
                        return " `" + this.key + "`=(`" + this.key + "`*?) ";
                    case div:
                        return " `" + this.key + "`=(`" + this.key + "`/?) ";
                }
            }
            return " `" + this.key + "`=? ";
        }

        public String getKey() {
            return "`" + this.key + "`";
        }
    }
}


