using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Newtonsoft.Json.Linq;
using System.IO;

namespace DataAccess
{
    public abstract class DALDataBase
    {
        public string globalError { set; get; }
        public List<JObject> globalMessages { set; get; }
        public string connectionString { set; get; }
        public JObject JQueryString { set; get; }

        public Int64 counter = 0;
        public Dictionary<string, Int64> tableNewIds = new Dictionary<string, Int64>();

        public abstract List<ReturnObject> Excute(List<CommandParam> commandParams, bool commit = false);

        public string DAL_TYPE { set; get; }

        public abstract ReturnObject Excute(CommandParam cmdParam, dynamic cmdConnectDynamic = null, dynamic cmdTransactDynamic = null);

        public dynamic DbNullIfNull(JProperty prop)
        {
            if (prop.Value.Type.ToString() == "Null")
            {
                return Convert.DBNull;
            }
            else
            {
                return prop.Value;
            }
        }

        public Dictionary<string, dynamic> DALBuildParams(
            FieldInfo[] dataParams,
            JObject args = null,
            JObject values = null
            )
        {

            // return null if no defined parameters or both data parameters are null
            if (dataParams == null || (values == null && args == null)) return null;

            Dictionary<string, dynamic> ret = new Dictionary<string, dynamic>();

            foreach (FieldInfo prm in dataParams)
            {
                if (!prm.isParameter) continue;

                bool paramFound = false;
                if (values != null)
                {
                    if (values.ContainsKey(prm.name))
                    {
                        // add to return value if parameter is one of the items in the values
                        paramFound = true;
                        ret.Add(prm.name, DALCastValue(prm.type, values[prm.name]));
                    }
                }
                if (!paramFound && args != null)
                {
                    if (args.ContainsKey(prm.name))
                    {
                        paramFound = true;
                        ret.Add(prm.name, DALCastValue(prm.type, args[prm.name]));
                    }
                }
            }

            return ret;

        }


        public dynamic DALCastValue(ColumnInfo col, dynamic paramValue = null)
        {
            return DALCastValue(col.type, paramValue);
        }
        public dynamic DALCastValue(string paramTypeString, dynamic paramValue = null, dynamic defaultValue = null)
        {
            if (paramValue == null) return defaultValue;
            switch (paramTypeString.ToLower())
            {
                case "string":
                    return Convert.ToString(paramValue);
                case "int64":
                    return Convert.ToInt64(paramValue);
                case "int32":
                    return Convert.ToInt32(paramValue);
                case "int16":
                    return Convert.ToInt16(paramValue);
                case "boolean":
                    return Convert.ToBoolean(Convert.ToString(paramValue).ToLower());
                case "decimal":
                case "double":
                    return Convert.ToDouble(paramValue);
                case "object":
                    break;
                case "date":
                case "datetime":
                    return Convert.ToDateTime(paramValue);
                default:
                    break;
            }
            return null;
        }

        public abstract Int64 GetScalar(string cmdText, Dictionary<string, dynamic> cmdParams = null);

        public abstract JArray GetJSONArray(CommandParam cmdParam);

        public abstract List<Dictionary<string, dynamic>> GetDictionaryArray(CommandParam cmdParam);

        public abstract dynamic GetDataReaderCommand(CommandParam cmdParam);

        public abstract DataTable GetDataTable(CommandParam cmdParam);

        public abstract ReturnObject GetRecordset(CommandParam cmdParam, bool returnFields = false, bool withFields = false,
            long pageNumber = 0, long pageSize = 0, JArray lookupParams = null);

        public abstract List<Dictionary<string, dynamic>> DALReaderToDictionary(dynamic rdr);

        public abstract JArray DALReaderToJSON(dynamic rdr);

        public abstract List<List<object>> DALReaderToList(dynamic rdr, long pageNumber = 0, long pageSize = 0, JArray lkpObj = null);

        public void LogMessage(string msg, string fileName = "Message.log", bool reset = false)
        {
            return;
            //string appPath = _g.APP_SETTINGS["APP_PATH"] + "\\" + _g.APP_SETTINGS["PATH_SETTINGS"];
            string appPath = AppDomain.CurrentDomain.BaseDirectory;
            string logFile = appPath + "\\" + fileName;
            if (reset) if (File.Exists(logFile)) File.Delete(logFile);
            File.AppendAllText(logFile, DateTime.Now + ": " + msg + "\r\n", Encoding.UTF8);
        }

        public void LogGlobalMessage(dynamic msg, string key = "",bool clear=false)
        {
            if (globalMessages == null)
            {
                globalMessages = new List<JObject>();
            }else
            {
                if (clear) globalMessages.Clear();
            }

            string dType = msg.GetType().ToString();

            if (dType.IndexOf("JObject") != -1)
                globalMessages.Add(msg);
            else if (dType.IndexOf("JArray") != -1)
                globalMessages.Add(new JObject() { [key.Length!=0 ? key : "array"] = msg });
            else
                globalMessages.Add(new JObject() { [key.Length != 0 ? key : "message"] = msg });
        }

        public bool isKeyExistInGlobalMessage(string key)
        {
            if (globalMessages == null) return false;
            return globalMessages.Find(obj => obj.ContainsKey(key)) != null;
        }

        public void writeAllText(string fn, string text)
        {
            if (File.Exists(fn)) File.Delete(fn);
            File.WriteAllText(fn, text);
        }


    }


    public class DALStamps
    {
        public DALStamps(List<ColumnInfo> columns, string userId = null)
        {

            DateTime tmpDt = DateTime.Now;
            stampDateTime = new DateTime(tmpDt.Year, tmpDt.Month, tmpDt.Day, tmpDt.Hour, tmpDt.Minute, tmpDt.Second);

            this.userId = userId;

            this.table = columns[0].table;

            this.updatedField = columns.Find(c => c.isUpdated);
            this.updatedByField = columns.Find(c => c.isUpdatedBy);

            this.createdField = columns.Find(c => c.isCreated);
            this.createdByField = columns.Find(c => c.isCreatedBy);

            this.lockedField = columns.Find(c => c.isLocked);
            this.lockedByField = columns.Find(c => c.isLockedBy);
        }

        private DALTable table { get; set; }

        public DateTime stampDateTime { get; set; }
        public string tableCode { get; set; }
        public Int64 keyValue { get; set; }
        public Int64 newKeyValue { get; set; }

        public string userId { get; set; }
        public ColumnInfo updatedField { get; set; }
        public ColumnInfo updatedByField { get; set; }
        public ColumnInfo createdField { get; set; }
        public ColumnInfo createdByField { get; set; }
        public ColumnInfo lockedField { get; set; }
        public ColumnInfo lockedByField { get; set; }
    }

    public static class SQLOperators
    {
        public const string
            or = "or",
            and = "and",
            eq = "=",
            lt = "<",
            lte = "<=",
            gt = ">",
            gte = ">=",
            inv = "in",
            btw = "between",
            like = "ALike";
    }

    //public class DALConnection
    //{
    //    public DALConnection()
    //    {

    //        try
    //        {
    //            // initialize and open a new connection
    //            activeConnection = new OleDbConnection(DALData.DAL.connectionString);
    //            if (activeConnection.State != ConnectionState.Open) activeConnection.Open();

    //            // attach transaction
    //            connectionTransaction = activeConnection.BeginTransaction();

    //            errorMessage = "";

    //        }
    //        catch (Exception e)
    //        {
    //            errorMessage = "InitError: " + e.Message;
    //        }

    //    }

    //    public void Commit()
    //    {
    //        try
    //        {
    //            connectionTransaction.Commit();
    //        }
    //        catch (Exception e)
    //        {
    //            errorMessage = "Commit Error: " + e.Message;
    //        }
    //        Dispose();
    //    }

    //    public void Rollback()
    //    {
    //        try
    //        {
    //            connectionTransaction.Rollback();
    //        }
    //        catch (Exception e)
    //        {
    //            errorMessage = "Rollback Error: " + e.Message;
    //        }
    //        Dispose();
    //    }

    //    public void Dispose()
    //    {
    //        try
    //        {
    //            if (connectionTransaction != null)
    //            {
    //                connectionTransaction.Dispose();
    //                connectionTransaction = null;
    //            }
    //            if (activeConnection != null)
    //            {
    //                if (activeConnection.State == ConnectionState.Open)
    //                {
    //                    activeConnection.Close();
    //                }
    //                activeConnection.Dispose();
    //                activeConnection = null;
    //            }

    //        }
    //        catch (Exception e)
    //        {
    //            errorMessage = "Dispose Error: " + e.Message;
    //        }

    //    }
    //}


}
