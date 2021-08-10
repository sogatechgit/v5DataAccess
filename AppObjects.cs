using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using _g = DataAccess.AppGlobals2;
using _g2 = DataAccess.DALGlobals;

namespace DataAccess
{
    class AppObjects
    {
    }

    public class AppReturn
    {
        public AppReturn()
        {
            recordCount = 0;
            recordsFormat = "array";
            result = "success";
            errorMessage = "";
            records = new JArray();
            props = new JObject();
            input = new JObject();
            headerProcessResult = new JObject();
            inputArr = new JArray();
            inputParams = new Dictionary<string, dynamic>();
            processLogs = new Dictionary<string, string>();

            returnStrings = new List<string> { };
            returnCode = "";
            returnType = "table";

            this.requestDateTime = DALGlobals.DateTimeNow;
            this.requestDuration = 0;
        }

        string recordsFormat { set; get; }
        public string returnDescription { set; get; }
        public string returnType { set; get; }
        public string subsKey { set; get; }
        public string returnCode { set; get; }
        public DateTime requestDateTime { set; get; }
        public long requestDuration { set; get; }
        public Int64 recordCount { set; get; }
        public JArray records { set; get; }

        public JObject returnDataParams { get; set; }
        public List<JObject> globalMesages { get; set; }

        public Dictionary<string, List<List<object>>> recordsProps { get; set; }
        public List<string> returnStrings { set; get; }
        public JObject props { set; get; }
        public JObject headerProcessResult { set; get; }
        public JObject invokeResult { set; get; }
        public JObject input { set; get; }
        public JArray inputArr { set; get; }
        public JArray columnsArr { set; get; }
        public List<string> fieldNames { set; get; }
        public DALStamps stamps { set; get; }
        public Dictionary<string, dynamic> inputParams { set; get; }
        public Dictionary<string, string> processLogs { set; get; }
        public string result { set; get; }
        public string errorMessage { set; get; }
        public List<ColumnInfo> columns { set; get; }
        public List<CommandParam> commands { set; get; }
        public List<List<object>> recordsList { get; set; }
        public Dictionary<string, dynamic> embeddedLookups { get; set; }

    }

    public static class AppDataset
    {
        /********************************************************************************************************
         * Configuration
         ********************************************************************************************************/
        public static string configPath { get; set; }
        public static string clientDevPath { get; set; }

        /*********************************************************************************************************
         *  Properties and variables declarations
         *********************************************************************************************************/

        // Initialize tables dictionary
        public static Dictionary<string, DALTable> AppTables = new Dictionary<string, DALTable>() { };

        // Initialize views dictionary
        public static Dictionary<string, DALView> AppViews = new Dictionary<string, DALView>() { };

        // Initialize stored procedures dictionary
        public static Dictionary<string, DALProcedure> AppProcedures = new Dictionary<string, DALProcedure>() { };

        // Initialize multipurpose ReturnObjectExternal
        public static ReturnObjectExternal GeneralRetObj = new ReturnObjectExternal();

        /*********************************************************************************************************
         *  Client-call methods
         *********************************************************************************************************/

        // Return data from  a Get request
        public static ReturnObject Post(string table, JObject values = null, JObject args = null)
        {
            return new ReturnObject();
        }

        // Return feedback data from  a Post request
        public static ReturnObject Get(string table, JObject args = null)
        {
            return new ReturnObject();
        }


        /*********************************************************************************************************
         *  Public methods
         *********************************************************************************************************/
        public static void Initialize(JObject args = null)
        {
            GetObjectConfigurations();
        }


        public static List<CommandParam> BuildCommandParamsListFromData(JObject values, JObject args = null)
        {

            List<CommandParam> ret = new List<CommandParam>();
            DALTableLink tableLink = new DALTableLink(_g.TKVStr(values, _g.KEY_TABLE_CODES), AppTables, args);

            JProperty data = _g.GetJPropery(values, "data");
            if (data == null) return null;

            string dataType = data.Value.Type.ToString().ToLower();

            JArray dataArr = new JArray();

            if (dataType == "array")
            {
                dataArr = (JArray)data.Value;
            }
            else
            {
                dataArr.Add((JObject)data.Value);
            }

            //JArray dataArr = (dataType == "array" ? data : new JArray() { data };


            foreach (JObject jo in dataArr)
            {
                Int64 parentKey = _g.TKV64(jo, tableLink.key);
                string[] action = tableLink.table.GetActionFromData(jo).Split('|');

                bool isNoAction = action[0] == _g.RES_NO_ACTION;
                bool isInsert = action[0] == "insert";
                bool isUpdate = action[0] == "update";
                bool isDelete = action[0] == "delete";

                if (isInsert)
                {
                    jo.Add(_g.KEY_NEWREC_TEMP_ID, parentKey);
                    // set new parent key
                    parentKey = Convert.ToInt64(action[1]);
                    jo[tableLink.key] = parentKey;
                }

                CommandParam cmdParam = new CommandParam();
                if (!isNoAction)
                {

                    cmdParam.cmdInput = new JObject();
                    foreach (JProperty jp in (JToken)jo)
                    {
                        if (jp.Name != tableLink.childCode)
                        {
                            cmdParam.cmdInput.Add(jp.Name, jp.Value);
                        }

                    }

                    if (isDelete)
                    {
                        // create command parameter if action is to be performed
                        cmdParam.cmdText = tableLink.table.SQLText(action[0]);
                        cmdParam.cmdParams = new Dictionary<string, dynamic>() { [tableLink.key] = parentKey };
                    }
                    else
                    {
                        DALTableFieldParams tblParams = new DALTableFieldParams(jo, tableLink.stamps, tableLink.table, isUpdate, null);
                        //cmdParam.tmpCols = tblParams.columns;
                        //cmdParam.tmpKeys = tblParams.keyFields;
                        cmdParam.cmdText = tblParams.SQLText;
                        cmdParam.cmdParams = tblParams.parameters;

                        if (isInsert)
                        {
                            cmdParam.cmdOutput = new JObject();
                            cmdParam.cmdOutput.Add(_g.KEY_NEWREC_TEMP_ID, _g.TKV64(jo, _g.KEY_NEWREC_TEMP_ID));
                            cmdParam.cmdOutput.Add(tableLink.key, parentKey);
                        }
                    }

                }   // end of NOT isNoAction
                else
                {
                    cmdParam.cmdText = "no action!";
                    cmdParam.cmdParams = new Dictionary<string, dynamic>() { [tableLink.key] = parentKey };
                }
                ret.Add(cmdParam);


                if (tableLink.hasChild && jo.ContainsKey(tableLink.childCode))
                {
                    // if parent JObject contains a property with name same as the child table code
                    JArray childArr = _g.TKVJArr(jo, tableLink.childCode);          // get children array

                    foreach (JObject cJo in childArr)
                    {

                        string[] childAction = tableLink.childTable.GetActionFromData(cJo).Split('|');
                        bool isChildNoAction = childAction[0] == _g.RES_NO_ACTION;
                        bool isChildInsert = childAction[0] == "insert";
                        bool isChildUpdate = childAction[0] == "update";
                        bool isChildDelete = childAction[0] == "delete";

                        Int64 childKey = _g.TKV64(cJo, tableLink.childKey);

                        if (isChildInsert)
                        {
                            // childAction[0]:action, childAction[1]:new AutoId

                            if (cJo.ContainsKey(tableLink.childParentKey)) cJo[tableLink.childParentKey] = parentKey;      // update parent key value
                            else cJo.Add(tableLink.childParentKey, parentKey);   // create new field token

                            cJo.Add(_g.KEY_NEWREC_TEMP_ID, childKey);           // add property containing the temporary id which will be used to find the matching record in the client-side

                            // set new child key
                            childKey = Convert.ToInt64(childAction[1]);
                            cJo[tableLink.childKey] = childKey;
                        }

                        // generate sql statements and parameters
                        CommandParam cmdChildParam = new CommandParam();
                        if (!isChildNoAction)
                        {

                            cmdChildParam.cmdInput = cJo;

                            if (isChildDelete)
                            {
                                // create command parameter if action is to be performed
                                cmdChildParam.cmdText = tableLink.table.SQLText(childAction[0]);
                                cmdChildParam.cmdParams = new Dictionary<string, dynamic>() { [tableLink.childKey] = childKey };
                            }
                            else
                            {
                                DALTableFieldParams tblChildParams = new DALTableFieldParams(cJo, tableLink.childStamps, tableLink.childTable, isChildUpdate, null);
                                //cmdChildParam.tmpCols = tblChildParams.columns;
                                //cmdChildParam.tmpKeys = tblChildParams.keyFields;
                                cmdChildParam.cmdText = tblChildParams.SQLText;
                                cmdChildParam.cmdParams = tblChildParams.parameters;

                                if (isChildInsert)
                                {
                                    cmdChildParam.cmdOutput = new JObject();
                                    cmdChildParam.cmdOutput.Add(_g.KEY_NEWREC_TEMP_ID, _g.TKV64(cJo, _g.KEY_NEWREC_TEMP_ID));
                                    cmdChildParam.cmdOutput.Add(tableLink.childKey, childKey);
                                }
                            }

                        }   // end of NOT isNoAction
                        else
                        {
                            cmdChildParam.cmdText = "no action!";
                            cmdChildParam.cmdParams = new Dictionary<string, dynamic>() { [tableLink.childKey] = childKey };
                        }
                        ret.Add(cmdChildParam);

                    }   // end of child foreach 

                }   // end of if hasChild and parent object contains a property named similar to the child table code





            }   // end of parent foreach


            return ret;
        }

        /*********************************************************************************************************
        *  Private methods
        *********************************************************************************************************/

        private static void GetObjectConfigurations()
        {
            SetTableConfiguration();
            SetViewConfiguration();
            SetProcedureConfiguration();
        }
        private static void SetViewConfiguration()
        {

            // clear DALView objects in the Dataset
            AppViews.Clear();
            // Set View Configurations
            string[] configFiles = Directory.GetFiles(_g.PATH_SETTINGS, _g.ConfigViewFile());


        }

        private static void SetProcedureConfiguration()
        {
            // clear DALProcedure objects in the Dataset
            AppProcedures.Clear();

            // Set Stored Procedure Configurations
            string[] configFiles = Directory.GetFiles(_g.PATH_SETTINGS, _g.ConfigProcedureFile());

        }

        private static string SetTableRelations(DALTable tbl)
        {
            tbl.tableProcessLogs.Add(tbl.tableCode + "_SetTableRelations", tbl.relations == null ? " no table.relations " : tbl.relations.Count().ToString());
            if (tbl.relations == null) return "";

            string retVal = "";

            foreach (JObject j in tbl.relations)
            {
                string type = _g.TKVStr(j, "type");
                string childTableCode = _g.TKVStr(j, "foreign_code");
                string[] childTableCodeArr = childTableCode.Split('-');
                DALTable childTable = AppTables[childTableCodeArr[0]];
                string childType = childTableCodeArr.Length == 1 ? "" : childTableCodeArr[1];

                if (tbl.tableRelations == null) tbl.tableRelations = new Dictionary<string, DALRelation>();

                DALRelation rel = new DALRelation(type, tbl, childTable,
                    _g.TKVStr(j, "local_field"), _g.TKVStr(j, "foreign_field"), _g.TKVBln(j, "parent_detail"), childType: childType);

                tbl.tableRelations.Add(childTableCode, rel);

            }

            return retVal;
        }

        private static void SetTableConfiguration()
        {
            // clear DALTable objects in the Dataset
            AppTables.Clear();
            // Get all table configuration files
            string[] configFiles = Directory.GetFiles(_g.PATH_SCHEMA_CONFIG, _g.PTN_TABLE_CONFIG);

            foreach (string fn in configFiles)
            {
                // Get Object Code that will be used as key to the element in the object dicationary
                string objCode = _g.GetCodeFromPattern(fn, _g.PTN_TABLE_CONFIG);

                // convert JSON string to JObject
                JObject jObject = JObject.Parse(File.ReadAllText(fn));

                // Convert JSON String of the columns property to JArray object
                JArray jCols = JArray.Parse(_g.TKVStr(jObject, "columns", "[]"));

                // Initialize new colsInfo list
                List<ColumnInfo> colsInfo = new List<ColumnInfo>() { };
                string fieldPrefix = _g.TKVStr(jObject, "tableFieldPrefix");

                // loop through columns if found in the configuration file
                if (jCols.Count != 0)
                {
                    foreach (JObject jCol in jCols)
                    {
                        colsInfo.Add(new ColumnInfo(
                                _g.TKVStr(jCol, "name")
                                , _g.TKVStr(jCol, "type","String")
                                , _g.TKVStr(jCol, "caption")
                                , _g.TKVStr(jCol, "alias")
                                , _g.TKVStr(jCol, "roles")
                                , _g.TKVInt(jCol, "keyPosition")
                                , _g.TKVInt(jCol, "uniquePosition")
                                , _g.TKVInt(jCol, "groupPosition")
                                , _g.TKVInt(jCol, "sortPosition")
                                , _g.TKVInt(jCol, "displayPosition")
                                , _g.TKVBln(jCol, "isRequired")
                                , _g.TKVBln(jCol, "isLong")
                                , fieldPrefix
                            ));
                    }

                }

                // Add new item in the Table dictionary
                AppTables.Add(
                    objCode, new DALTable(
                        _g.TKVStr(jObject, "tableName")
                        , colsInfo
                        , description: _g.TKVStr(jObject, "description")
                        , tableCode: objCode
                        , tableClassFilename: _g.TKVStr(jObject, "tableClassFilename")
                        , tableClass: _g.TKVStr(jObject, "tableClass")
                        , tableRowClass: _g.TKVStr(jObject, "tableRowClass")
                        , tableFieldPrefix: _g.TKVStr(jObject, "tableFieldPrefix")
                        , links: _g.TKVJArr(jObject, "links")
                        , relations: _g.TKVJArr(jObject, "relations")
                        , captions: _g.TKVJObj(jObject, "captions")
                        , tableLinks: _g.TKVJArr(jObject, "tableLinks")
                        , tableLinksFields: _g.TKVJObj(jObject, "tableLinksFields")
                        , tableCollection: AppTables
                        , clientConfig: _g.TKVJObj(jObject, "clientConfig")
                    )
                 );

            }   // end of foreach configFiles

            bool clientSideDevExist = File.Exists(_g.PATH_TARGET_TYPESCRIPT_DATASET);

            // iterate through tables to generate single client-side typescript file
            string typeScript = "";
            string importScript = "";
            string instanceScript = "";
            string tblClassName;

            string relScript = "";

            // Iterate through all generated DALTable objects to perform
            // Table post creation property assignments

            for (int idx = 0; idx < AppTables.Count(); idx++)
            {
                DALTable tblObj = AppTables.ElementAt(idx).Value;

                SetTableRelations(tblObj);
                if (tblObj.tableRelations != null)
                {
                    string relFmt = "\n    this.t{0}.tableRelations.push(new Relation(\"{1}\", \"{2}\", this.t{3}, this.t{4}, \"{5}\", \"{6}\", {7}));";
                    foreach (string relKey in tblObj.tableRelations.Keys)
                    {
                        DALRelation rel = tblObj.tableRelations[relKey];
                        relScript += String.Format(relFmt, tblObj.tableClass.Substring(1), rel.foreignTableCode,
                            rel.type, rel.table.tableClass.Substring(1), rel.tableChild.tableClass.Substring(1),
                            rel.localField, rel.foreignField, rel.parentDetail.ToString().ToLower());
                    }
                    //this.tblAnomalies.tableRelations["an"]=new Relation("lnk",this.ds.tblAnomalies,this.ds.tblFailureThreats);
                }

                if (idx == 0) typeScript = tblObj.templateImports;
                typeScript += "\n\n\n" + tblObj.templateClass;

                tblClassName = tblObj.tableClass;

                importScript += "\n" + _g.TPL_TARGET_TYPESCRIPT_IMPORT
                    .Replace("<TABLE>", tblClassName)
                    .Replace("<TABLEROW>", tblObj.tableRowClass);

                instanceScript += "\n  " + _g.TPL_TARGET_TYPESCRIPT_INSTANCE
                    .Replace("<TABLEVAR>", tblClassName.Substring(0, 1).ToLower() + tblClassName.Substring(1))
                    .Replace("<TABLE>", tblClassName);

            }

            // write typecript to client side script file
            if (clientSideDevExist)
            {
                DALData.DAL.writeAllText(_g.PATH_TARGET_TYPESCRIPT_PATH, typeScript);

                string dsts = File.ReadAllText(_g.PATH_TARGET_TYPESCRIPT_DATASET);
                // write in between INCLUDES
                //<INCLUDES>
                //</INCLUDES>

                string[] tsArr = _g.Split(dsts, "//<INCLUDES>");
                string[] tsArr2 = _g.Split(tsArr[1], "//</INCLUDES>");
                string fmt = "//<INCLUDES>{0}\n//</INCLUDES>";

                dsts = tsArr[0] + String.Format(fmt, importScript) + tsArr2[1];

                tsArr = _g.Split(dsts, "//<INSTANTIATE>");
                tsArr2 = _g.Split(tsArr[1], "//</INSTANTIATE>");

                fmt = "//<INSTANTIATE>{0}\n//</INSTANTIATE>";
                dsts = tsArr[0] + String.Format(fmt, instanceScript) + tsArr2[1];

                //if (relScript.Length != 0)
                //{
                tsArr = _g.Split(dsts, "//<RELATIONS>");
                tsArr2 = _g.Split(tsArr[1], "//</RELATIONS>");

                fmt = "//<RELATIONS>{0}\n//</RELATIONS>";
                dsts = tsArr[0] + String.Format(fmt, relScript) + tsArr2[1];
                //}

                tsArr = _g.Split(dsts, "//<DECLARE>");
                tsArr2 = _g.Split(tsArr[1], "//</DECLARE>");

                string declareString = String.Format("\n  this.apiCommon.PARAMS_DELIM_CHAR = '{0}';", _g2.PARAMS_DELIM_CHAR) +
                        String.Format("\n  this.apiCommon.PARAMS_VAL_DELIM_CHAR = '{0}';", _g2.PARAMS_VAL_DELIM_CHAR) +
                        String.Format("\n  this.apiCommon.FIELD_PARENT_LINK_ALIAS = '{0}';", _g2.FIELD_PARENT_LINK_ALIAS) +
                        String.Format("\n  this.apiCommon.FIELD_CHILD_FIRST_ALIAS = '{0}';", _g2.FIELD_CHILD_FIRST_ALIAS) +
                        String.Format("\n  this.apiCommon.FIELD_CHILD_COUNT_ALIAS = '{0}';", _g2.FIELD_CHILD_COUNT_ALIAS);

                fmt = "//<DECLARE>{0}\n  //</DECLARE>";
                dsts = tsArr[0] + String.Format(fmt, declareString) + tsArr2[1];

                DALData.DAL.writeAllText(_g.PATH_TARGET_TYPESCRIPT_DATASET, dsts);
            }

            GeneralRetObj.debugStrings.Add("Total Tables: " + AppTables.Count());

            // set change track table property for non-change track tables...

            if (AppTables.ContainsKey(_g.KEY_TABLE_UPDATE_TRACK_CODE))
            {
                DALTable chgTrack = AppTables[_g.KEY_TABLE_UPDATE_TRACK_CODE];
                foreach (KeyValuePair<string, DALTable> tbl in AppTables)
                {
                    tbl.Value.tableChangeTrack = chgTrack;
                }
            }


        }   // end of SetTableConfiguration method

    }

}

