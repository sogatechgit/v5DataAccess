using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.OleDb;
using _g = DataAccess.DALGlobals;
using Newtonsoft.Json.Linq;
using System.Diagnostics;

namespace DataAccess
{
    public class DALDataOleDb : DALDataBase
    {
        public DALDataOleDb() { DAL_TYPE = "JET"; }

        public override List<ReturnObject> Excute(List<CommandParam> commandParams, bool commit = false)
        {
            // Execute collection of commands using a single transaction object

            List<ReturnObject> retVal = new List<ReturnObject>();
            if (commandParams.Count() == 0) return retVal;

            // Creat a common connection/transaction object to 
            // be used when executing commands
            DALOleDbConnection cnn = new DALOleDbConnection(true);

            CommandParam cmdPrm = null;

            try
            {
                bool errorEncountered = false;
                foreach (CommandParam cp in commandParams)
                {
                    cmdPrm = cp;
                    // pass common connection and transaction when executing individual commands
                    ReturnObject ret = Excute(cp, cnn.Connection, cnn.Transaction);

                    JObject retData = new JObject();

                    retData.Add("tempKey", cp.tempKey);
                    retData.Add("newKey", cp.newKey);
                    retData.Add("cmdOutput", cp.cmdOutput);

                    ret.result.returnDataParams = retData;
                    if (cp.table != null) ret.returnCode = cp.table.tableCode;


                    // check if error was encountered when executing individual command,

                    // If error is enountered, return Exception message and Rollback all 
                    // transactions prior to the error.

                    retVal.Add(ret);
                    errorEncountered = ret.result.result == _g.RES_ERROR;

                    // exit for loop if exception is encountered
                    if (errorEncountered) break;

                    ret.result.returnObject = null;

                }

                // if error has not occur during execution of individual commands, 
                // commit changes and dispose connection and transaction objects
                // using the Commit method of the connection object (DALOleDbConnection)
                if (commit && !errorEncountered)
                    cnn.Commit();   // commit and dispose
                else
                {
                    // raise exception
                    throw new Exception("Error posting record, " + (cmdPrm != null ? cmdPrm.cmdText : ""));
                }

            }
            catch (Exception e)
            {
                // if error occured, rollback and dispose connection and transaction objects
                // by calling the Rollback method of the connection object (DALOleDbConnection)
                if (cnn != null) cnn.Rollback();

                ReturnObject errRet = new ReturnObject();
                errRet.returnType = _g.RES_ERROR;
                errRet.result.exceptionMessage = e.Message;
                errRet.result.result = _g.RES_ERROR;
                retVal.Add(errRet);

                // return an error message
                // return "Error posting multiple updates: " + e.Message;
            }


            return retVal;
        }

        public override ReturnObject Excute(CommandParam cmdParam,
               dynamic cmdConnectDynamic = null, dynamic cmdTransactDynamic = null)
        {
            /*************************************************
             * Execute individual command
             *************************************************/

            ReturnObject ret = new ReturnObject();
            DALOleDbConnection cnn = null;
            ret.result.affectedRecords = -5;
            globalError = "";
            try
            {
                OleDbConnection cmdConnect = null;
                OleDbTransaction cmdTransact = null;

                if (cmdConnectDynamic != null) cmdConnect = (OleDbConnection)cmdConnectDynamic;
                if (cmdTransactDynamic != null) cmdTransact = (OleDbTransaction)cmdTransactDynamic;

                bool withConnParam = true;

                // if common connection and transaction is not supplied
                if (cmdConnect == null)
                {
                    // if no connection parameter is passed
                    cnn = new DALOleDbConnection();
                    cmdConnect = cnn.Connection;
                    cmdTransact = cnn.Transaction;

                    // set connection flag to be used during cleanup process
                    withConnParam = false;
                }

                // open connection if still closed
                if (cmdConnect.State != ConnectionState.Open) cmdConnect.Open();

                // initialize command object
                string cmdText = cmdParam.cmdText;
                OleDbCommand cmd = new OleDbCommand(cmdText, cmdConnect);

                // set command transaction object
                if (cmdTransact != null) cmd.Transaction = cmdTransact;

                // add parameters to command object
                foreach (string key in cmdParam.cmdParams.Keys)
                {
                    cmd.Parameters.Add(key, cmdParam.cmdParams[key]);
                }

                // if passed command text is not a SQL statement, CommandType is StoredProcedure, else Text
                cmd.CommandType = (cmdText.IndexOf(" ") == -1 ? CommandType.StoredProcedure : CommandType.Text);

                // Execute Command
                ret.result.affectedRecords = cmd.ExecuteNonQuery();

                Int64 cnt = cmd.Parameters.Count;

                // cleanup
                // connection was initiated within this method
                if (!withConnParam && cnn != null) cnn.Commit();


                ret.result.result = _g.RES_SUCCESS;
                ret.result.exceptionMessage = "";

            }
            catch (Exception e)
            {
                // Execute rollback only if connection was initiated within this method
                if (cnn != null) cnn.Rollback();

                globalError = e.Message;
                ret.result.result = _g.RES_ERROR;
                ret.result.exceptionMessage = "Execute(...) [single]: " + e.Message;
            }

            return ret;

        }

        public override Int64 GetScalar(string cmdText, Dictionary<string, dynamic> cmdParams = null)
        {
            OleDbConnection cmdConnect = null;
            globalError = "";
            try
            {
                cmdConnect = new OleDbConnection(connectionString);
                cmdConnect.Open();

                OleDbCommand cmd = new OleDbCommand(cmdText, cmdConnect);
                cmd.CommandType = CommandType.Text;

                // add parameters to command object
                if (cmdParams != null)
                    foreach (string key in cmdParams.Keys)
                        cmd.Parameters.Add(key, cmdParams[key]);

                Int64 ret = Convert.ToInt64(cmd.ExecuteScalar());

                cmdConnect.Close();
                cmdConnect.Dispose();

                return ret;

            }
            catch (Exception e)
            {
                globalError = e.Message;
                if (cmdConnect != null)
                {
                    if (cmdConnect.State == ConnectionState.Open) cmdConnect.Close();
                    cmdConnect.Dispose();
                }
                return -1;
            }
        }


        public override JArray GetJSONArray(CommandParam cmdParam)
        {
            OleDbCommand cmd = (OleDbCommand)GetDataReaderCommand(cmdParam);
            if (cmd == null) return null;

            JArray jArr = null;
            OleDbDataReader rdr = null;

            try
            {
                rdr = cmd.ExecuteReader();
                jArr = DALReaderToJSON(rdr);

            }
            catch (Exception e)
            {
                jArr = null;
            }
            finally
            {
                if (cmd != null)
                {
                    if (rdr != null) rdr.Close();
                    if (cmd.Connection != null)
                        if (cmd.Connection.State == ConnectionState.Open) cmd.Connection.Close();
                    cmd.Dispose();

                }
            }

            return jArr;
        }

        public override List<Dictionary<string, dynamic>> GetDictionaryArray(CommandParam cmdParam)
        {
            OleDbCommand cmd = (OleDbCommand)GetDataReaderCommand(cmdParam);

            if (cmd == null) return null;

            List<Dictionary<string, dynamic>> dArr = null;
            OleDbDataReader rdr = null;

            try
            {
                rdr = cmd.ExecuteReader();
                dArr = DALReaderToDictionary(rdr);

            }
            catch (Exception e)
            {
                dArr = null;
            }
            finally
            {
                if (cmd != null)
                {
                    if (rdr != null) rdr.Close();
                    if (cmd.Connection != null)
                        if (cmd.Connection.State == ConnectionState.Open) cmd.Connection.Close();
                    cmd.Dispose();
                }
            }

            return dArr;
        }


        public override dynamic GetDataReaderCommand(CommandParam cmdParam)
        {
            OleDbConnection cmdConnect = new OleDbConnection(connectionString);
            if (cmdConnect.State != ConnectionState.Open) cmdConnect.Open();

            string cmdText = cmdParam.cmdText;
            OleDbCommand cmd = new OleDbCommand(cmdText, cmdConnect);

            try
            {
                // create parameters
                if (cmdParam.cmdParams != null)
                {
                    foreach (string key in cmdParam.cmdParams.Keys)
                    {
                        cmd.Parameters.Add(key, cmdParam.cmdParams[key]);

                    }
                }

                if (cmdText.IndexOf(" ") == -1)
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    cmd.CommandType = CommandType.Text;
                }

            }
            catch (Exception e)
            {
                if (cmdConnect != null)
                    if (cmdConnect.State == ConnectionState.Open) cmdConnect.Close();
                cmd.Dispose();
                cmd = null;
            }

            return cmd;
        }

        public override DataTable GetDataTable(CommandParam cmdParam)
        {
            OleDbConnection cmdConnect = new OleDbConnection(connectionString);
            cmdConnect.Open();
            string cmdText = cmdParam.cmdText;
            OleDbCommand cmd = new OleDbCommand(cmdText, cmdConnect);

            // create parameters
            if (cmdParam.cmdParams != null)
            {
                foreach (string key in cmdParam.cmdParams.Keys)
                {
                    cmd.Parameters.Add(key, cmdParam.cmdParams[key]);

                }
            }

            // check command type
            if (cmdText.IndexOf(" ") == -1)
            {
                cmd.CommandType = CommandType.StoredProcedure;
            }
            else
            {
                cmd.CommandType = CommandType.Text;
            }

            OleDbDataReader rdr = cmd.ExecuteReader();

            DataTable ret = new DataTable();
            ret.Load(rdr);

            cmdConnect.Close();
            cmdConnect.Dispose();

            return ret;
        }

        public override ReturnObject GetRecordset(CommandParam cmdParam,
            bool returnFields = false,
            bool withFields = false,
            long pageNumber = 0, long pageSize = 0, JArray lookupParams = null)
        {

            DALOleDbConnection cnn = new DALOleDbConnection();
            OleDbConnection cmdConnect = cnn.Connection;

            bool withConnParam = true;
            ReturnObject returnValue = new ReturnObject();

            string[] inlineLookupFieldsArr = InlineLookupFields(lookupParams);

            try
            {
                if (cmdConnect == null)
                {
                    withConnParam = false;
                    cmdConnect = new OleDbConnection(connectionString);
                }
                if (cmdConnect.State != ConnectionState.Open) cmdConnect.Open();

                string cmdText = cmdParam.cmdText;

                // OracleCommand - will require cmdText not terminated with a semicolon ";"
                OleDbCommand cmd = new OleDbCommand(cmdText, cmdConnect);


                // create parameters
                if (cmdParam.cmdParams != null)
                {
                    foreach (string key in cmdParam.cmdParams.Keys)
                    {
                        cmd.Parameters.Add(key, cmdParam.cmdParams[key]);
                    }
                }


                if (cmdText.IndexOf(" ") == -1)
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    cmd.CommandType = CommandType.Text;
                }
                OleDbDataReader rdr = cmd.ExecuteReader();

                // build fields list and return as .fields parameter
                if (cmd.CommandType == CommandType.StoredProcedure || returnFields)
                {
                    returnValue.result.fields = new List<FieldInfo>();
                    returnValue.result.returnString = "=>";
                    try
                    {
                        using (var schemaTable = rdr.GetSchemaTable())
                        {
                            returnValue.result.debugStrings.Add("Table Schema:");
                            returnValue.result.debugStrings.Add("- Schema column count:" + schemaTable.Columns.Count.ToString());
                            returnValue.result.debugStrings.Add("- Table column count:" + schemaTable.Rows.Count.ToString());
                            returnValue.result.debugStrings.Add("- Columns Information:");
                            foreach (DataColumn c in schemaTable.Columns)
                            {
                                returnValue.result.debugStrings.Add("    p:" + c.ColumnName + ", " + c.DataType.ToString());
                            }
                            foreach (DataColumn c in schemaTable.Columns)
                            {
                                returnValue.result.returnString += c.ColumnName + ", ";
                            }
                            foreach (DataRow row in schemaTable.Rows)
                            {
                                string ColumnName = row.Field<string>("ColumnName");
                                dynamic DataTypeName = row.Field<dynamic>("DataType").Name;
                                short NumericPrecision = row.Field<short>("NumericPrecision");
                                short NumericScale = row.Field<short>("NumericScale");
                                int ColumnSize = row.Field<int>("ColumnSize");
                                bool IsLong = row.Field<bool>("IsLong");

                                returnValue.result.debugStrings.Add("**** " + ColumnName + " ****");
                                returnValue.result.debugStrings.Add("-    DataTypeName-" + DataTypeName);
                                returnValue.result.debugStrings.Add("-    NumericPrecision-" + NumericPrecision);
                                returnValue.result.debugStrings.Add("-    NumericScale-" + NumericScale);
                                returnValue.result.debugStrings.Add("-    ColumnSize-" + ColumnSize);
                                returnValue.result.debugStrings.Add("-    IsLong-" + _g.BlnToStr(IsLong));

                                //returnValue.result.returnString += row.Table.Columns.Count.ToString();

                                returnValue.result.fields.Add(new FieldInfo()
                                {
                                    isParameter = false,
                                    isLong = IsLong,
                                    name = ColumnName,
                                    type = DataTypeName,
                                    size = ColumnSize
                                });

                            }
                        }
                    }
                    catch (Exception e)
                    {
                        returnValue.result.returnString += e.Message;
                    }

                    returnValue.result.returnString += "<=";

                }

                for (int fidx = 0; fidx < rdr.FieldCount; fidx++)
                {
                    // this value will be assigned to the result's fieldNames object.
                    // only add fields that is not a displayField in a lookup definition
                    string fldName = rdr.GetName(fidx);
                    if (lookupParams != null)
                    {
                        //add only the fields that do not belong to the inline lookup 
                        //if (lookupParams.SelectToken("$[?(@.displayField == '" + fldName + "')]") == null)
                        //    returnValue.result.fieldsNames.Add(fldName);
                        if(!inlineLookupFieldsArr.Contains(fldName)) returnValue.result.fieldsNames.Add(fldName);
                    }
                    else
                    {
                        // add all field names from the dataReader
                        returnValue.result.fieldsNames.Add(fldName);
                    }
                }

                if (withFields)
                {
                    returnValue.result.jsonReturnData = DALReaderToJSON(rdr);
                    returnValue.result.recordCount = returnValue.result.jsonReturnData.Count;
                }
                else
                {
                    List<List<object>> retList = DALReaderToList(rdr, pageNumber, pageSize, lookupParams);
                    if (retList.Count > 0)
                    {
                        returnValue.result.recordCount = retList.Count - 1;
                        returnValue.result.returnDataParams = JObject.Parse(retList.ElementAt(0).ElementAt(0).ToString());

                    }
                    else
                    {
                        returnValue.result.recordCount = 0;
                        returnValue.result.returnDataParams = new JObject();

                    }


                    if (returnValue.result.recordCount > 0)
                        returnValue.result.returnData = retList.GetRange(1, (int)returnValue.result.recordCount);
                    // get field names

                }


            }
            catch (Exception e)
            {
                //returnValue = mark +", "+  e.Message;
                returnValue.result.result = _g.RES_ERROR;
                returnValue.result.error = e.Message;
            }
            finally
            {
                // cleanup
                //if (cmdConnect != null)
                //    if (cmdConnect.State == ConnectionState.Open)
                //        if (!withConnParam) cmdConnect.Close();

                if (cnn != null) cnn.Dispose();

            }

            return returnValue;

        }

        string[] InlineLookupFields(JArray lookupParams)
        {
            if (lookupParams == null) return null;

            string inlineLookupFields = "";

            for (int lkpIdx = 0; lkpIdx < lookupParams.Count; lkpIdx++)
            {
                JObject lkp = (JObject)lookupParams.ElementAt(lkpIdx);
                inlineLookupFields += ((inlineLookupFields.Length != 0 ? "," : "") + lkp["displayField"].ToString());

                // split sub lookup field list and add to inlineLookupFields collection
                string[] dspSubArr = (lkp["displayFieldSub"].ToString()).Split(SQLJoinChars.ARGUMENTS_SEPARATOR);
                if (dspSubArr.Length != 0)
                {
                    for (int dspSubIdx = 0; dspSubIdx < dspSubArr.Length; dspSubIdx++)
                    {
                        string dspFld = dspSubArr[dspSubIdx].Trim();
                        if (dspFld == "") continue;
                        inlineLookupFields += "," + dspFld;
                    }
                }
            }

            return inlineLookupFields.Split(SQLJoinChars.ARGUMENTS_SEPARATOR);

        }

        public override List<Dictionary<string, dynamic>> DALReaderToDictionary(dynamic rdr)
        {
            OleDbDataReader reader = (OleDbDataReader)rdr;

            List<Dictionary<string, dynamic>> ret = new List<Dictionary<string, dynamic>>();

            if (!reader.HasRows) return ret;

            int rdrFieldCount = reader.FieldCount;

            string[] fieldNames = new string[rdrFieldCount];
            for (int idx = 0; idx < rdrFieldCount; idx++)
            {
                fieldNames[idx] = reader.GetName(idx);
            }
            object fldVal;
            string tmp = "";

            while (reader.Read())
            {
                Dictionary<string, dynamic> rowObj = new Dictionary<string, dynamic>();
                for (int idx = 0; idx < rdrFieldCount; idx++)
                {
                    fldVal = reader.GetValue(idx);

                    if (fldVal == Convert.DBNull)
                    {
                        rowObj.Add(fieldNames[idx], null);
                    }
                    else
                    {
                        rowObj.Add(fieldNames[idx], (dynamic)fldVal);
                    }
                }
                ret.Add(rowObj);
            }

            return ret;
        }

        public override JArray DALReaderToJSON(dynamic rdr)
        {
            OleDbDataReader reader = (OleDbDataReader)rdr;

            JArray ret = new JArray();

            if (!reader.HasRows) return ret;

            int rdrFieldCount = reader.FieldCount;

            string[] fieldNames = new string[rdrFieldCount];
            for (int idx = 0; idx < rdrFieldCount; idx++) { fieldNames[idx] = reader.GetName(idx); }
            object fldVal;

            while (reader.Read())
            {
                JObject rowObj = new JObject();
                for (int idx = 0; idx < rdrFieldCount; idx++)
                {
                    fldVal = reader.GetValue(idx);

                    if (fldVal == Convert.DBNull)
                    {
                        rowObj.Add(fieldNames[idx], null);
                    }
                    else
                    {
                        rowObj.Add(fieldNames[idx], (dynamic)fldVal);
                    }
                }
                ret.Add(rowObj);
            }

            return ret;
        }

        public override List<List<object>> DALReaderToList(dynamic rdr, long pageNumber = 0, long pageSize = 0, JArray lkpObj = null)
        {
            // Returns a List of List of objects representing the data collected from the reader object
            // this is also where inline lookup obects are formed lookup
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            string[] lookupParamFieldsArr = InlineLookupFields(lkpObj);
            long rCnt_B = 0;
            long rCnt = 0;

            List<List<object>> ret = new List<List<object>>();
            try
            {
                OleDbDataReader reader = (OleDbDataReader)rdr;
                

                int fieldCount = reader.FieldCount;
                object fldVal;



                long pageBookmarkRecord = (pageNumber - 1) * pageSize;
                long pageEndRecord = pageBookmarkRecord + pageSize;


                if (reader.HasRows)
                {
                    
                    List<int> fldIndex = new List<int>();
                    Dictionary<string, JObject> lkpDict = null;


                    if (lkpObj != null)
                    {
                        //lkpObj.SelectToken("$[?(@.valueField == 'AN_STATUS')]")!=null
                        lkpDict = new Dictionary<string, JObject>();
                        for (int idx = 0; idx < fieldCount; idx++)
                        {
                            // fldIndex.Add(idx);
                            string fldName = reader.GetName(idx);
                            string valFld;
                            string dspFld;
                            string dspFldSub;
                            JObject lkpDef = (JObject)lkpObj.SelectToken("$[?(@.displayField == '" + fldName + "')]");

                            if (lkpDef != null)
                            {
                                // lookup definition is using the field as displayField. therefore remove from the selection set
                                // and build lookup list out of the unique values when looping through the loop
                                valFld = (string)lkpDef["valueField"];
                                dspFld = (string)lkpDef["displayField"];
                                dspFldSub = (string)lkpDef["displayFieldSub"];

                                if (dspFldSub.Length != 0)
                                {
                                    string[] dspFldSubArr = dspFldSub.Split(SQLJoinChars.ARGUMENTS_SEPARATOR);
                                    JObject dspFldSubIdx = new JObject();
                                    for (int subIdx = 0; subIdx < dspFldSubArr.Length; subIdx++)
                                    {
                                        string dspFldName = dspFldSubArr[subIdx];
                                        dspFldSubIdx.Add(dspFldName, reader.GetOrdinal(dspFldName));
                                    }
                                    lkpDef.Add("dspFldSubIdx", dspFldSubIdx);                        // display field index
                                }

                                lkpDef.Add("valueIndex", reader.GetOrdinal(valFld));    // value field index
                                lkpDef.Add("displayIndex", idx);                        // display field index

                                lkpDict.Add(valFld, lkpDef);
                            }
                            else
                            {
                                // add as return value
                                // only include fields that are not in the lookup param fields in value extraction
                                if(!lookupParamFieldsArr.Contains(fldName)) fldIndex.Add(idx);
                            }
                        }
                    }
                    else
                    {
                        for (int idx = 0; idx < fieldCount; idx++) fldIndex.Add(idx);
                    }

                    while (reader.Read())
                    {
                        if (pageSize == 0 || (rCnt >= pageBookmarkRecord && rCnt < pageEndRecord))
                        //if (pageSize == 0 || (rCnt_B >= pageBookmarkRecord && rCnt_B < pageEndRecord))
                        {
                            // read field values
                            List<object> rowObj = new List<object>();
                            object dbNullObj = Convert.DBNull;
                            for (int idx = 0; idx < fldIndex.Count; idx++)
                            {
                                int fIdx = fldIndex[idx];
                                fldVal = reader.GetValue(fIdx);

                                if (fldVal == dbNullObj)
                                {
                                    rowObj.Add(null);
                                }
                                else
                                {
                                    rowObj.Add(fldVal);
                                }
                            }

                            // append object array to the return value and will determine the number of records processed
                            ret.Add(rowObj);

                            // update lookup if any
                            if (lkpDict != null)
                            {
                                string debugTip = "";

                                try
                                {
                                    foreach (string lKey in lkpDict.Keys)
                                    {
                                        JObject lkpCfg = lkpDict[lKey];
                                        JObject lkp = (JObject)lkpCfg["lookup"];
                                        //string key = "k" + reader.GetValue((int)lkpCfg["valueIndex"]);
                                        string key = "" + reader.GetValue((int)lkpCfg["valueIndex"]);

                                        debugTip += "," + lKey + ", " + key;
                                        //if (key != "k")
                                        if (key != "")
                                            if (!lkp.ContainsKey(key))
                                            {
                                                string lkpVal = (string)reader.GetValue((int)lkpCfg["displayIndex"]);
                                                if (lkpCfg.ContainsKey("dspFldSubIdx"))
                                                {
                                                    foreach(JProperty jp in lkpCfg["dspFldSubIdx"])
                                                    {
                                                        lkpVal += "|" + (string)reader.GetValue((int)jp.Value);
                                                    }
                                                }
                                                lkp.Add(key, lkpVal);
                                            }
                                                
                                    }

                                }
                                catch (Exception rdrErr)
                                {
                                    string errText = rdrErr.Message;
                                }
                            }
                        };
                        rCnt_B++;
                        rCnt++;     // total records from the query
                    }

                    // append summary information at the end of the return list

                    reader.Close();

                    stopwatch.Stop();

                    JObject jSumm = new JObject()
                    {
                        ["pageNumber"] = pageNumber,
                        ["pageSize"] = pageSize,
                        ["totalPages"] = (pageSize > 0 ? (rCnt / pageSize) + ((rCnt % pageSize) > 0 ? 1 : 0) : 0),
                        ["bookmarkStart"] = pageBookmarkRecord,
                        ["bookmarkEnd"] = pageEndRecord,
                        ["recordCount"] = ret.Count,
                        ["totalRecords_B"] = rCnt_B,    // needed to add this property to address the issue of
                                                        // client-side property not reflecting the correct number
                        ["totalRecords"] = rCnt_B, //rCnt,
                        ["inlineLookups"] = lkpObj,
                        ["duration"] = stopwatch.ElapsedMilliseconds,
                        ["serverStamp"] = DateTime.Now
                    };
                    ret.Insert(0, new List<object>() { jSumm });
                    //ret.Add(new List<object>() { jSumm });
                }
            }
            catch (Exception e)
            {
                string err = e.Message;
            }

            return ret;
        }

        public void BuildNodeLocation(int parentId, string parentCode = "", OleDbConnection cn = null, string chars = "")
        {

            // parentId - root node id
            // parentCode - root TRE_NOD_LOC value

            // get all immediate children
            OleDbConnection cnObj;
            OleDbCommand cmd;
            OleDbDataReader rdr;

            if (cn == null)
            {
                cnObj = new OleDbConnection(connectionString);
                cnObj.Open();

                cmd = new OleDbCommand("select lchars from sys_LocationCharacters order by chr_order;", cnObj);
                cmd.CommandType = CommandType.Text;

                rdr = cmd.ExecuteReader();
                while (rdr.Read()) chars += (string)rdr.GetValue(0);

                rdr.Close();
                cmd.Dispose();

                if (parentCode.Length == 0) parentCode = chars.Substring(0, 1) + chars.Substring(0, 1);
            }
            else
                cnObj = cn;

            if (cnObj.State != ConnectionState.Open) cnObj.Open();

            string treTableName = AppDataset.AppTables["tre"].tableName;

            if (cn == null)
            {
                cmd = new OleDbCommand(String.Format("update {0} set tre_nod_loc=" + DALData.PARAM_PREFIX + "p1 where tre_nod_tag=" + DALData.PARAM_PREFIX + "p2;", treTableName), cnObj);
                cmd.Parameters.Add(new OleDbParameter(DALData.PARAM_PREFIX + "p1", parentCode));
                cmd.Parameters.Add(new OleDbParameter(DALData.PARAM_PREFIX + "p2", parentId));
                cmd.CommandType = CommandType.Text;
                cmd.ExecuteNonQuery();
                cmd.Dispose();
            }

            cmd = new OleDbCommand(String.Format("select tre_nod_tag from {0} where tre_nod_tag_par=" + DALData.PARAM_PREFIX + "p1", treTableName), cnObj);
            cmd.CommandType = CommandType.Text;
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new OleDbParameter(DALData.PARAM_PREFIX + "p1", parentId));
            rdr = cmd.ExecuteReader();

            int chIdx1 = 0;
            int chIdx2 = 0;

            while (rdr.Read())
            {

                string chCode = parentCode + chars.Substring(chIdx1, 1) + chars.Substring(chIdx2, 1);
                int cid = Convert.ToInt32(rdr.GetValue(0));

                OleDbCommand ucmd = new OleDbCommand(String.Format("update {0} set tre_nod_loc=" + DALData.PARAM_PREFIX + "p1 where tre_nod_tag=" + DALData.PARAM_PREFIX + "p2;", treTableName), cnObj);
                ucmd.Parameters.Clear();
                ucmd.Parameters.Add(new OleDbParameter(DALData.PARAM_PREFIX + "p1", chCode));
                ucmd.Parameters.Add(new OleDbParameter(DALData.PARAM_PREFIX + "p2", cid));
                ucmd.CommandType = CommandType.Text;
                ucmd.ExecuteNonQuery();
                ucmd.Dispose();

                if (chIdx2 < chars.Length - 1)
                {
                    chIdx2++;
                }
                else
                {
                    chIdx2 = 0;
                    chIdx1++;
                }

                BuildNodeLocation(cid, chCode, cnObj, chars);
            }

            rdr.Close();
            if (cn == null && cnObj != null)
            {
                // ending
                cnObj.Close();
                cnObj.Dispose();
            }

        }

    }

    class DALOleDbConnection
    {
        public DALOleDbConnection(bool beginTransaction = false)
        {
            // Creates a new connection with Transaction initiated
            try
            {
                // Create new connection
                this.Connection = new OleDbConnection(DALData.DAL.connectionString);
                this.Connection.Open();

                // Set connection transaction
                this.Transaction = (beginTransaction ? Connection.BeginTransaction() : null);

                ErrorMessge = "";
            }
            catch (Exception e)
            {
                ErrorMessge = "Error Initializing Connection: " + e.Message;
            }

        }

        public void Commit(bool closeConnection = true)
        {
            if (Transaction == null) return;
            try
            {
                Transaction.Commit();
                Dispose(closeConnection);
            }
            catch (Exception e)
            {
                ErrorMessge = "Error committing changes: " + e.Message;
                Dispose();
            }
        }

        public void Rollback(bool closeConnection = true)
        {
            if (Transaction == null) return;
            try
            {
                Transaction.Rollback();
                Dispose(closeConnection);
            }
            catch (Exception e)
            {
                ErrorMessge = "Error rolling back changes: " + e.Message;
                Dispose();
            }
        }

        public void Dispose(bool closeConnection = true)
        {
            if (Transaction != null)
            {
                Transaction.Dispose();
                Transaction = null;
            }
            if (Connection != null)
            {
                if (Connection.State == ConnectionState.Open) Connection.Close();
                Connection.Dispose();
                Connection = null;
            }
        }

        public OleDbConnection Connection { set; get; }
        public OleDbTransaction Transaction { set; get; }

        public string ErrorMessge { set; get; }

    }

}