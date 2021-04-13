using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Data;
using Newtonsoft.Json.Linq;
using _g = DataAccess.DALGlobals;
using System.Diagnostics;

namespace DataAccess
{


    public class DALRelation
    {
        public DALRelation(string _type, DALTable table, DALTable tableChild,
            string localField = "", string foreignField = "", bool parentDetail = false, string childType = "")
        {
            this.type = _type;
            this.table = table;
            this.tableChild = tableChild;
            this.foreignTableCode = tableChild.tableCode;

            this.localField = localField;
            this.foreignField = foreignField;
            this.parentDetail = parentDetail;

            this.childType = childType;
        }

        public DALTable table { get; set; }            // Owner Data Table
        public DALTable tableChild { get; set; }            // Linked Data Table

        public string type { get; set; }                // TableRelationTypes class
        public string localField { get; set; }    // code assigned to the linked table
        public string foreignField { get; set; }    // code assigned to the linked table
        public string childType { get; set; }    // code assigned to the linked table
        public string foreignTableCode { get; set; }    // code assigned to the linked table
        public bool parentDetail { get; set; }    // code assigned to the linked table

        //this.foreignField = _g.TKVStr(args, "foreign_field");

        public List<CommandParam> CreateNewLinkCommandParams(Int64 parentId, string childIds, bool clear = true,bool simultaneous=false)
        {
            // parentId - key value of the record from the parent table
            // childIds - comma-delimited string specifying the key value from the child table
            // clear - remove all existing link records under the parent record

            List<CommandParam> cmds = new List<CommandParam>();

            // DELETE: add delete command to the cmds command collection
            cmds.Add(CreateDeleteLinkCommandParam(parentId, clear ? null : childIds));

            string[] childIdsArr = childIds.Split(',');
            string cond = "";
            Dictionary<string, dynamic> prms = new Dictionary<string, dynamic>() { { table.PARAM_PREFIX + "p0", parentId } };

            string sqlTemplate = "insert into [{0}] ({1}, {2}) " +
                     "select " + table.PARAM_PREFIX + "p0" + table.FIELD_ALIAS_LINK + "{3}, {4}" + table.FIELD_ALIAS_LINK + "{5} " +
                     (simultaneous ? ";" : "from {6} " + "where ({7});");

            // INSERT : add insert command to cdms command collection

            if (simultaneous)
            {
                prms.Add(table.PARAM_PREFIX + "p1", Convert.ToInt64(childIdsArr[0]));
                cmds.Add(new CommandParam(String.Format(sqlTemplate,

                                                linkTableName,
                                                linkTableFieldA,
                                                linkTableFieldB,
                                                linkTableFieldA,
                                                tableChild.keyCol.name,
                                                linkTableFieldB
                                            ),
                                            prms
                                          )
                        );  //cmds.Add(...)

            }
            else
            {
                for (int i = 1; i <= childIdsArr.Length; i++)
                {
                    // build condition clause
                    cond += (cond.Length != 0 ? " Or " : "") + String.Format("{0}=" + table.PARAM_PREFIX + "p{1}",
                            tableChild.keyCol.name, i);
                    // build parameters dictionary from index 1 to length of the children
                    prms.Add(String.Format(table.PARAM_PREFIX + "p{0}", i), Convert.ToInt64(childIdsArr[i - 1]));
                }

                /* INSERT INTO lnk_an_rf ( an_rf_ida, an_rf_idb )
                   SELECT 203 AS an_rf_ida, C.RF_ID
                   FROM tbl_ReferenceFiles AS C
                   WHERE (((C.RF_ID)=8236 Or (C.RF_ID)=5187));
                   0 - link table name
                   1 - link parent key field
                   2 - link child key field
                   3 - link parent key field
                   4 - child table key field (from tableChild = tableChild.keyCol.name)
                   5 - link child key field
                   6 - tableChild.tableName
                   7 - condition build from the childIds parameter
                 */

                cmds.Add(new CommandParam(String.Format(sqlTemplate,

                                                linkTableName,
                                                linkTableFieldA,
                                                linkTableFieldB,
                                                linkTableFieldA,
                                                tableChild.keyCol.name,
                                                linkTableFieldB,
                                                tableChild.tableName,
                                                cond
                                            ),
                                            prms
                                          )
                        );  //cmds.Add(...)


            }


            // add insert command




            return cmds;
        }

        public CommandParam CreateDeleteLinkCommandParam(Int64 parentId, string childIds = null)
        {
            List<CommandParam> cmds = new List<CommandParam>();
            if (childIds == null)
            {
                // delete all linked records under the specified parent id
                return new CommandParam(
                    String.Format("delete from [{0}] where {1} = " + table.PARAM_PREFIX + "p0;", linkTableName, linkTableFieldA),
                    new Dictionary<string, dynamic>() { { table.PARAM_PREFIX + "p0", parentId } }, _table: this.table);

            }
            else
            {
                // delete records of specified childIds
                string[] childIdsArr = childIds.Split(',');

                string cond = "";
                Dictionary<string, dynamic> prms = new Dictionary<string, dynamic>() { { table.PARAM_PREFIX + "p0", parentId } };

                for (int i = 1; i <= childIdsArr.Length; i++)
                {
                    cond += (cond.Length != 0 ? " Or " : "") + String.Format("{0}=" + table.PARAM_PREFIX + "p{1}", linkTableFieldB, i);
                    prms.Add(String.Format(table.PARAM_PREFIX + "p{0}", i), Convert.ToInt64(childIdsArr[i - 1]));
                }

                return new CommandParam(String.Format("delete from [{0}] where ({1} = " + table.PARAM_PREFIX + "p0 And ({2}));",
                                        linkTableName, linkTableFieldA, cond), prms, _table: this.table);
            }
        }

        public JArray GetLinkedRecords(Int64 parentId = 0)
        {
            JArray ret = new JArray();

            // if parentId == 0, get all link records in array format
            /*  [
             *      [parentId1, childId1],
             *      [parentId2, childId2],
             *      [..., ...],
             *      [parentId#, childId#]
             *  ]
             */

            return null;
        }

        public string linkTableName
        {
            get
            {
                if (this.type == TableRelationTypes.LINK)
                    return (TableRelationTypes.LINK + "_" + this.table.tableCode + "_" + foreignTableCode + (childType.Length!=0 ? "-" + childType : "")).ToUpper();

                if (this.type == TableRelationTypes.ONE2ONE) return table.tableName;

                return "";
            }
        }

        public string linkTableFieldA
        {
            get
            {
                if (this.type == TableRelationTypes.LINK)
                    return (table.tableCode + "_" + foreignTableCode + "_ida").ToUpper();

                if (this.type == TableRelationTypes.ONE2ONE)
                    return table.keyCol.name;

                return "";
            }
        }


        public string linkTableFieldB
        {
            get
            {
                if (this.type == TableRelationTypes.LINK)
                    return (table.tableCode + "_" + foreignTableCode + "_idb").ToUpper();
                if (this.type == TableRelationTypes.ONE2ONE)
                    return this.localField;



                return "";
            }
        }


        public string linkFromClause
        {
            get
            {
                return String.Format("[{0}] AS L INNER JOIN {1} AS T ON L.{2} = T.{3}",
                        linkTableName, tableChild.tableName, linkTableFieldB, tableChild.keyCol.name);

            }
        }

        //
        public string selectAgregate(string aggregate, string aggregateField = "")
        {
            return String.Format("(select top 1 {0}({1}) from {2} where {3}=T.{4})",
                aggregate, aggregateField.Length > 0 ? aggregateField : tableChild.keyCol.name, tableChild.tableName, foreignField, localField.Length != 0 ? localField : table.keyCol.name);
        }

        public string countSelectAgregate()
        {
            return String.Format("(select top 1 count({0}) from {1} where {2}=T.{3})",
                tableChild.keyCol.name, tableChild.tableName, foreignField, table.keyCol.name);
        }

    }

    public class LinkedFilter
    {
        public LinkedFilter(string linkFilterCode, DALTable table)
        {
            string tblCode = table.tableCode;

            isReverseLink = linkFilterCode.StartsWith(SQLJoinCStr.LINK_LEFT_FILTER_SYMBOL);

            // parent table parameters
            linkLocalTableCode = tblCode;
            linkLocalTableKey = table.keyCol.name;

            // linkFilterCode - <|><linkTableCode>
            string[] codeArr = linkFilterCode.Substring(1).Split('-');
            string childType = codeArr.Length == 1 ? "" : codeArr[1];
            linkTableCode = codeArr[0];


            // resolve link tablename
            xFix = String.Format("{0}_{1}", isReverseLink ? linkTableCode : tblCode, isReverseLink ? tblCode : linkTableCode);
            linkTableName = String.Format("lnk_{0}" + (childType.Length != 0 ? "-" + childType : ""), xFix).ToUpper();

            // resolve table field
            linkFilterField = String.Format(xFix + "_id{0}", isReverseLink ? "a" : "b").ToUpper();
            linkLocalField = String.Format(xFix + "_id{0}", isReverseLink ? "b" : "a").ToUpper();
        }

        private bool isReverseLink = false;
        public string xFix = "";
        public string linkTableCode = "";
        public string linkLocalTableCode = "";
        public string linkLocalTableKey = "";
        public string linkTableName = "";
        public string linkFilterField = "";
        public string linkLocalField = "";

        public string GetFilterFromClause(string oldFromClause)
        {
            int fromMarker = oldFromClause.IndexOf("from ");
            string ret = oldFromClause;
            if (fromMarker != -1) ret = oldFromClause.Substring(fromMarker + 5);
            ret = String.Format(" from ({0}) INNER JOIN [{1}] as {2} on {3}.{4}={5}.{6}",
                ret, linkTableName, xFix, xFix, linkLocalField, linkLocalTableCode, linkLocalTableKey);
            return ret;
        }
        public string GetFilterSelectClause(string oldSelectClause)
        {
            string ret = String.Format("{0}, {1}.{2}", oldSelectClause, xFix, linkFilterField);
            return ret;
        }

    }
    public class DALLinkObj
    {
        public DALLinkObj(JObject args)
        {
            this.localField = _g.TKVStr(args, "local_field");
            this.foreignField = _g.TKVStr(args, "foreign_field");

            this.type = _g.TKVStr(args, "link_type", _g.LNK_OTO);
            this.fields = _g.TKVStr(args, "fields", this.type == _g.LNK_OTO ? _g.LNK_NO_STAMP : "");

            this.prefix = _g.TKVStr(args, "prefix");
            this.childCode = _g.TKVStr(args, "child_code");

            this.groupKey = _g.TKV64(args, "group_key", -1);

            if (this.fields == "") this.fields = _g.LNK_NO_STAMP;


        }
        public string foreignField { get; }
        public string localField { get; }
        public string childCode { get; }
        public Int64 groupKey { get; }
        public string type { get; }
        public string fields { get; }
        public string prefix { get; }
    }

    public class DALTable
    {
        /**************************************************************************************
        * Created By: Archangel Villarojo
        * Last Updated: 2020-04-13
        * Description: Data Table class
        * Methods / Function ----------------------------------------------------------------
        * 
        * Properties ------------------------------------------------------------------------
        * Events ----------------------------------------------------------------------------
        **************************************************************************************/
        public DALTable(
            string tableName = "",
            List<ColumnInfo> columns = null,
            string description = "",
            string tableCode = "",
            string tableClassFilename = "",
            string tableClass = "",
            string tableRowClass = "",
            string tableFieldPrefix = "",
            JArray links = null,
            JArray relations = null,
            JObject captions = null,
            JArray tableLinks = null,
            JObject tableLinksFields = null,
            Dictionary<string, DALTable> tableCollection = null,
            bool isLinkTable = false,
            bool autoKey = true,
            JObject clientConfig = null)
        {

            this.tableName = tableName;
            this.tableCode = tableCode;
            this.description = description;
            this.clientConfig = clientConfig;

            this.links = links;

            this.relations = relations;
            this.tableRelations = null;

            this.captions = captions;
            this.tableLinks = tableLinks;
            this.tableLinksFields = tableLinksFields;
            this.autoKey = autoKey;
            this.log = new List<string>();

            this.tableChangeTrack = null;

            if (columns == null) columns = new List<ColumnInfo>();

            this.columns = columns;

            this.tableClassFilename = tableClassFilename.Length == 0 ? this.tableName : tableClassFilename;
            this.tableFieldPrefix = tableFieldPrefix.Length == 0 ? this.tableName : tableFieldPrefix;
            this.tableClass = tableClass.Length == 0 ? this.tableName : tableClass;
            this.tableRowClass = tableRowClass.Length == 0 ? this.tableName.TrimEnd('s') + "Row" : tableRowClass;

            this.tableCollection = tableCollection;
            this.tableProcessLogs = new Dictionary<string, string>();

            this.isLinkTable = isLinkTable;

            this.instantiated = DateTime.Now;

            Initialize();

            // setup columns indices
            if (this.columnsIndex != null) this.columnsIndex.Clear();
            columnsIndex = new Dictionary<string, ColumnInfo>();
            foreach (ColumnInfo col in columns)
            {
                this.columnsIndex.Add(col.name, col);
            }

        }

        public string DATE_STRING_FORMAT
        {
            /*
             * Date Expression:
             * Oracle SELECT TO_CHAR(b, 'YYYY/MM/DD') AS fmtDate
             * Oracle SELECT TO_CHAR(b, 'yyyy-mm-dd') AS fmtDate
             * Jet SELECT Format(b, 'YYYY/MM/DD') AS fmtDate
             */

            get
            {
                return DALData.DAL.DAL_TYPE == "ORACLE" ? "TO_CHAR({0}.{1}, 'yyyy-mm-dd')" : "Format({0}.{1},'yyyy-mm-dd')";
            }
        }

        public string YEAR_STRING_FORMAT
        {
            /*
             * Date Expression:
             * Oracle SELECT TO_CHAR(b, 'YYYY/MM/DD') AS fmtDate
             * Oracle SELECT TO_CHAR(b, 'yyyy-mm-dd') AS fmtDate
             * Jet SELECT Format(b, 'YYYY/MM/DD') AS fmtDate
             */

            get
            {
                return DALData.DAL.DAL_TYPE == "ORACLE" ? "TO_CHAR({0}.{1}, 'yyyy')" : "Format({0}.{1},'yyyy')";
            }
        }
        public string DAY_STRING_FORMAT
        {
            /*
             * Date Expression:
             * Oracle SELECT TO_CHAR(b, 'YYYY/MM/DD') AS fmtDate
             * Oracle SELECT TO_CHAR(b, 'yyyy-mm-dd') AS fmtDate
             * Jet SELECT Format(b, 'YYYY/MM/DD') AS fmtDate
             */

            get
            {
                return DALData.DAL.DAL_TYPE == "ORACLE" ? "TO_CHAR({0}.{1}, 'dd')" : "Format({0}.{1},'dd')";
            }
        }
        public string DATE_VALUE_CALL
        {
            /*
             * Date Expression:
             * Oracle TO_DATE(field)
             * Jet DateValue(dateField)
             */

            get
            {
                //return DALData.DAL.DAL_TYPE == "ORACLE" ? "TO_DATE({0}.{1})" : "DateValue({0}.{1})";
                return DALData.DAL.DAL_TYPE == "ORACLE" ? "TO_CHAR({0}.{1}, 'yyyy-mm-dd')" : "Format({0}.{1},'yyyy-mm-dd')";
            }
        }


        public string ALIAS_LEFT_DELIM
        {
            get
            {
                return DALData.DAL.DAL_TYPE == "ORACLE" ? "\"" : "[";
            }
        }
        public string ALIAS_RIGHT_DELIM
        {
            get
            {
                return DALData.DAL.DAL_TYPE == "ORACLE" ? "\"" : "]";
            }
        }

        public string SQL_LIKE
        {
            get
            {
                return DALData.DAL.DAL_TYPE == "ORACLE" ? "like" : "alike";
            }
        }

        public string TABLE_ALIAS_LINK
        {
            get
            {
                return DALData.DAL.DAL_TYPE == "ORACLE" ? " " : " as ";
            }
        }
        public string FIELD_ALIAS_LINK
        {
            get
            {
                return DALData.DAL.DAL_TYPE == "ORACLE" ? " " : " as ";
            }
        }
        public string PARAM_PREFIX
        {
            get
            {
                return DALData.DAL.DAL_TYPE == "ORACLE" ? ":" : "@";
            }
        }

        public DateTime instantiated { set; get; }
        public string tableName { set; get; }
        public string tableFieldPrefix { set; get; }

        public string tableClassFilename { set; get; }
        public string tableClass { set; get; }      // to be used as model name in 
        public string tableRowClass { set; get; }      // to be used as model name in 
        public string tableCode { set; get; }
        public JObject clientConfig { set; get; }

        public JArray relations { set; get; }
        public Dictionary<string, DALRelation> tableRelations { set; get; }
        public Dictionary<string, string> tableProcessLogs { set; get; }

        public JArray links { set; get; }
        public JObject captions { set; get; }
        public JArray tableLinks { set; get; }
        public JObject tableLinksFields { set; get; }

        public DALTable tableChangeTrack { set; get; }
        public string description { set; get; }
        public List<string> log { set; get; }

        private string _templateString = "";
        public string templateString
        {
            get
            {
                if (_templateString == "")
                {
                    _templateString = "/***********************************************************************" +
                        "\n* Automatically generated on " + DateTime.Now.ToString() +
                        "\n***********************************************************************/\n\n" +
                        File.ReadAllText(appTemplateFile, Encoding.UTF8);
                }
                return _templateString;
            }
        }

        public string templateImports
        {
            get
            {

                // get template string
                return templateString.Substring(0, templateString.IndexOf("//TEMPLATE START"));
            }
        }
        public string templateClass
        {
            get
            {

                string tableProperties = "public tableFieldPrefix=\"" + tableFieldPrefix + "\";" +
                    "\r\n\tprivate _tableLinks:Array<string> = " + tableLinks.ToString() + ";" +
                    "\r\n\tprivate _links:Array<any> = " + links.ToString() + ";" +
                    "\r\n\tpublic clientConfig:any = " + this.clientConfig.ToString() + ";";

                string tableColumns = "this.tableCode=\"" + tableCode + "\";\r\n";
                string rowProperties = "";
                string fmtField = "\r\n\t\tpublic {0}?:{1}";
                string fmtColumn = "\r\n\tthis.columns.push(" +
                    "new ColumnInfo('{0}', '{1}', '{2}', '{3}', '{4}', " +
                        "{5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, this)" +
                   ");";
                /* 0 - field name
                 * 1 - type
                 * 2 - caption
                 * 3 - alias
                 * 4 - roles
                 * 5 - key position
                 * 6 - unique position
                 * 7 - group position
                 * 8 -sort position
                 * 9 - display position
                 * 10 - is parameter
                 * 11- is required
                 * 12- is long
                 * 13 parent table object
                 */
                foreach (ColumnInfo col in this.columns)
                {
                    rowProperties += (rowProperties.Length != 0 ? ", " : "") +
                        String.Format(fmtField, col.name, MapDataTypeToTS(col.type));

                    tableColumns += String.Format(fmtColumn,
                        col.name,
                        MapDataTypeToTS(col.type),
                        col.caption,
                        col.alias,
                        col.roles,

                        col.keyPosition,
                        col.uniquePosition,
                        col.groupPosition,
                        col.sortPosition,
                        col.displayPosition,

                        _g.BlnToStr(col.isParameter),
                        _g.BlnToStr(col.isRequired),
                        _g.BlnToStr(col.isLong)

                    );
                }

                string linkedFields = "";
                int fCtr = 0;

                if (links != null)
                {
                    foreach (JObject lnk in links)
                    {
                        DALLinkObj lnkObj = new DALLinkObj(lnk);
                        fCtr += 1;
                        if (lnkObj.type == _g.LNK_OTO || lnkObj.type == _g.LNK_LKP)
                        {
                            DALTable childTable = tableCollection[lnkObj.childCode];
                            string rowClass = childTable.tableRowClass;
                            //string rowMethod =  (lnkObj.prefix.Length!=0 ? lnkObj.prefix : lnkObj.childCode)  + "_" + lnkObj.localField;
                            string rowMethod = "_p" + fCtr.ToString();
                            string getMethod = "_g" + fCtr.ToString();

                            Int64 groupId = lnkObj.groupKey;

                            /*
                             * 		private get _f():TblLookupRow { return this.ChildRow('lkp','par_lkp_id_b',118); }
		private _f2(fn:string):any { return this._f ? this._f[fn] : null}
	// cols count : 21, nostamp
		public get lkpb_lkp_grp_id():number{return this._f2("lkp_grp_id")}
                             */

                            linkedFields += "\n\n\tprivate get {method}():{class} { return this.ChildRow('{tbl}','{field}',{group});}\n\tprivate {getmethod}(fn:string):any { return this.{method}? this.{method}[fn] : null}\n\n"
                                        .Replace("{method}", rowMethod)
                                        .Replace("{getmethod}", getMethod)
                                        .Replace("{class}", rowClass)
                                        .Replace("{field}", lnkObj.localField)
                                        .Replace("{group}", groupId.ToString())
                                        .Replace("{tbl}", lnkObj.childCode);

                            // select all fields by default;
                            List<ColumnInfo> cols = childTable.columns;

                            if (lnkObj.fields != "*")  // selected fields only, else include all fields
                            {
                                if (lnkObj.fields == _g.LNK_NO_STAMP)
                                {
                                    // exclude stamp fields
                                    cols = cols.Where(f => !f.isStampField).ToList();
                                }
                                else
                                {
                                    // specific columns specified
                                    string fieldList = "," + lnkObj.fields + ",";
                                    cols = cols.Where(f => fieldList.IndexOf("," + f.name + ",") != -1).ToList();
                                }
                            }

                            linkedFields += " // cols count : " + cols.Count() + ", " + lnkObj.fields + "\n";

                            // set template string which includes get and set properties, but set property is only
                            // applicable is link is not lookup
                            string tpl = "\tpublic get {fldm}():{type}{return this.{getmethod}('{fld}')}\n" +
                                (lnkObj.type == _g.LNK_LKP ? "" : "\tpublic set {fldm}(value:{type}) { this.{method}.{fld} = value}\n\n");

                            foreach (ColumnInfo col in cols)
                            {
                                string colName = (lnkObj.prefix.Length != 0 ? lnkObj.prefix + "_" : "") + col.name;

                                linkedFields += tpl.Replace("{method}", rowMethod)
                                    .Replace("{getmethod}", getMethod)
                                    .Replace("{fldm}", colName)
                                    .Replace("{fld}", col.name)
                                    .Replace("{type}", MapDataTypeToTS(col.type));
                            }
                        }   // end of one to one type
                    }   // end of for each link
                }   // end of links not null

                return templateString.Substring(templateString.IndexOf("//TEMPLATE START") + 16)
                    .Replace("//TABLE_DECLARATIONS", tableProperties)       // replace table declaration placeholder
                    .Replace("//CONSTRUCTOR_CALLS", tableColumns)           // replace constructor calls placeholder
                    .Replace("TABLE_ROW_PROPERTIES", rowProperties)         // replace row constructor properties
                    .Replace("TABLE_CLASS", this.tableClass)                // replace tableClass name
                    .Replace("TABLE_ROW_CLASS", this.tableRowClass)        // replace tableRowClass name
                    .Replace("//TABLE_ROW_CONSTRUCTOR_CALLS", linkedFields);
            }
        }

        public bool autoKey { set; get; }

        public List<ColumnInfo> columns { set; get; }
        public Dictionary<string, ColumnInfo> columnsIndex { set; get; }
        public Dictionary<string, DALTable> tableCollection { set; get; }

        private List<ColumnInfo> _keyCols = null;
        public List<ColumnInfo> keyCols
        {
            get
            {
                if (_keyCols == null)
                    _keyCols = columns.Where(f => f.keyPosition != -1).OrderBy(f => f.keyPosition).ToList();
                return _keyCols;
            }
        }
        private List<ColumnInfo> _sortCols = null;
        public List<ColumnInfo> sortCols
        {
            get
            {
                if (_sortCols == null)
                    _sortCols = columns.Where(f => f.sortPosition != -1).OrderBy(f => f.sortPosition).ToList();
                return _sortCols;
            }
        }

        private List<ColumnInfo> _groupCols = null;
        public List<ColumnInfo> groupCols
        {
            get
            {
                if (_groupCols == null)
                    _groupCols = columns.Where(f => f.groupPosition != -1).OrderBy(f => f.groupPosition).ToList();
                return _groupCols;
            }
        }
        private List<ColumnInfo> _uniqueCols = null;
        public List<ColumnInfo> uniqueCols
        {
            get
            {
                if (_uniqueCols == null)
                    _uniqueCols = columns.Where(f => f.uniquePosition != -1).OrderBy(f => f.uniquePosition).ToList();
                return _uniqueCols;
            }
        }
        private string globalError
        {
            get
            {
                return DALData.DAL.globalError;
            }
        }
        private void clearGlobalError()
        {
            DALData.DAL.globalError = "";
        }

        private Int64 _NewAutoId = -1;
        public Int64 NewAutoId
        {
            get
            {
                if (!this.autoKey) return -1;
                if (_NewAutoId == -1)
                {
                    _NewAutoId = DALData.DAL.GetScalar("SELECT Max(" + keyCols.ElementAt(0).name + " + 1) FROM " + this.tableName);
                }
                else
                {

                    _NewAutoId++;
                }

                return _NewAutoId;
            }
        }


        private JObject RefNoItems = new JObject();
        public string NewRefNo(string refField, string refFormat, int year = -1, bool reset = false)
        {
            // currDate - yyyy-mm-dd[Thh:mm:ss]
            // refField - field name to get and put data from/to
            // refFormat - pattern ressembling final data format where year and numeric reference placeholder is specified
            try
            {

                const string YEAR_PATTERN = "yy";
                const char NUMBER_PATTERN = 'n';

                // convert format to lower case to make sure it conforms with the requirement for 
                // YEAR_PATTERN and NUMBER_PATTERN search
                refFormat = refFormat.ToLower();

                string yearStr = new string('0', YEAR_PATTERN.Length) + (year == -1 ? DateTime.Now.ToString(YEAR_PATTERN) : year.ToString());
                yearStr = yearStr.Substring(yearStr.Length - YEAR_PATTERN.Length, YEAR_PATTERN.Length);

                string searchPattern = refFormat.Replace(YEAR_PATTERN, yearStr).Replace(NUMBER_PATTERN, '_');

                string lastRef = "";

                int yearIndex = refFormat.IndexOf(YEAR_PATTERN);
                bool process = reset || !RefNoItems.ContainsKey(refFormat);

                if (!process) process = (RefNoItems.GetValue(refFormat).ToString().Substring(yearIndex, YEAR_PATTERN.Length) != yearStr);

                int firstNumberIndex = refFormat.IndexOf(NUMBER_PATTERN);
                int lastNumberIndex = refFormat.LastIndexOf(NUMBER_PATTERN);
                int numLen = lastNumberIndex - firstNumberIndex + 1;


                if (!process)
                {
                    // last reference exists and valid as reference for the current year
                    lastRef = RefNoItems.GetValue(refFormat).ToString();

                }
                else
                {
                    DataTable tbl = DALData.DAL.GetDataTable(new CommandParam(String.Format("select max({0})" + FIELD_ALIAS_LINK + "mx from {1} where {2} " + SQL_LIKE + " " + PARAM_PREFIX + "p1", refField, this.tableName, refField), new List<dynamic>() { searchPattern }));
                    DataRow row = tbl.Rows[0];
                    lastRef = row[0].ToString();
                }

                int currRefNum = (lastRef.Length == 0 ? 0 : Convert.ToInt32(lastRef.Substring(firstNumberIndex, (numLen))));

                string pads = new String('0', numLen);
                string Ns = new String(NUMBER_PATTERN, numLen);

                string newRefNumFmt = pads + (currRefNum + 1).ToString();
                newRefNumFmt = newRefNumFmt.Substring(newRefNumFmt.Length - numLen, numLen);

                string newRefCode = refFormat.Replace(YEAR_PATTERN, yearStr).Replace(Ns, newRefNumFmt);

                RefNoItems[refFormat] = newRefCode;

                return newRefCode;

            }
            catch (Exception e)
            {
                return "Error getting " + e.Message;
            }


        }

        public ColumnInfo keyCol
        {
            // returns the first element of the key column list
            get
            {
                List<ColumnInfo> cols = this.keyCols;
                if (cols.Count() == 0)
                {
                    return null;
                }
                else
                {
                    return cols[0];
                }

            }
        }

        public ColumnInfo grpCol
        {
            // returns the first element of the group column list
            get
            {
                List<ColumnInfo> cols = this.groupCols;
                if (cols.Count() == 0)
                {
                    return null;
                }
                else
                {
                    return cols[0];
                }

            }
        }


        // switch properties
        private bool _isDataTable;
        public bool isDataTable { set { _isDataTable = value; _isLinkTable = !value; } get { return _isDataTable; } }
        private bool _isLinkTable;
        public bool isLinkTable { set { _isLinkTable = value; _isDataTable = !value; } get { return _isLinkTable; } }


        public string appDataPath { get { return _g.APP_SETTINGS["PATH_SETTINGS"]; } }
        public string appSchemaFile
        {
            get
            {

                return (_g.APP_SETTINGS["PATH_TARGET_TYPESCRIPT_FOLDER"] != "" ? _g.APP_SETTINGS["PATH_TARGET_TYPESCRIPT_FOLDER"] : _g.APP_SETTINGS["PATH_SCHEMA_CLIENT"]) + "\\" + String.Format(_g.APP_SETTINGS["FMT_TABLE_MODEL"], this.tableClassFilename);
            }
        }
        public string appTemplateFile { get { return _g.APP_SETTINGS["PATH_SCHEMA_TEMPLATES"] + "\\template.table.ts"; } }

        private void Initialize()
        {

            if (!isFileExists() || true)
            {
                if (isFileExists()) File.Delete(appSchemaFile);

                // Extract all table columns ...
                CollectAllColumns();


                // //TABLE_ROW_CONSTRUCTOR_CALLS 

                // File.WriteAllText(appSchemaFile, templateImports + "\n\n" + templateClass, Encoding.UTF8);  // create new class typescript file

            }
        }


        private void CollectAllColumns()
        {
            // open recordset without any record, just to return all the fields in the data table
            ReturnObject retTbl = DALData.DAL.GetRecordset(
                new CommandParam("select * from " + this.tableName + " where 1=2;", new List<dynamic>() { false }),
                returnFields: true
             );

            this.log = retTbl.result.debugStrings;

            foreach (FieldInfo f in retTbl.result.fields)
            {
                // if column is not yet defined ...

                ColumnInfo ci = null;
                if (columns.Count(c => c.name == f.name) == 0)
                {
                    // field is not yet in the columns collection
                    ci = new ColumnInfo(f.name, f.type, isLong: f.isLong, prefix: tableFieldPrefix);
                    columns.Add(ci);
                }
                else
                {
                    // column already exist, just update the type
                    ci = columns.First(c => c.name == f.name);
                    if (ci != null) ci.type = f.type;
                }

                if (ci != null)
                {
                    // serch add caption
                    if (this.captions.ContainsKey(f.name))
                        ci.caption = _g.TKVStr(this.captions, f.name);

                    // set column's owner table
                    ci.table = this;
                }

            }



        }

        public string GetActionFromData(JObject data)
        {
            /**********************************************************
             * Returns SQL action based on the data shape 
             * 1. update if ...
             *     a) key field value is a positive Int64
             *     b) record has more than a single non-JArray token/property
             * 2. insert if ...
             *     a) key field value is a negative Int64
             * 3. delete if ...
             *     a) key field value is a positive Int64
             *     b) record has only has the key field as a single token
             **********************************************************/
            string key = keyCols.ElementAt(0).name;

            // return empty string if data does not contain the key field as one of the tokens
            if (!data.ContainsKey(key)) return "";

            string ret = "";
            Int64 keyValue = _g.TKV64(data, key);

            // return insert if key value is negative
            if (keyValue < 0) ret = String.Format(SQLModes.INSERT + "|{0}", this.NewAutoId);

            // return delete if key is the lone token and has a negative value
            if (ret.Length == 0) if (data.PropertyValues().Count() == 1) if (keyValue > 0) ret = SQLModes.DELETE;

            // return update if key field has a positive value and with non-Array token
            // besides the key field
            if (ret.Length == 0)
            {
                foreach (JProperty jp in (JToken)data)
                {
                    if (jp.Name != key)
                    {
                        if (jp.Value.Type.ToString() != "Array")
                        {
                            ret = SQLModes.UPDATE;
                            break;
                        }
                    }
                }
            }

            if (ret.Length == 0) ret = _g.RES_NO_ACTION;

            return ret;

        }

        public bool colExist(string name, Dictionary<string, ColumnInfo> index = null)
        {
            if (index == null) index = columnsIndex;
            return index.ContainsKey(name);
        }


        private void GenerateTypeScriptSchema()
        {

        }

        private void ExtractActualColumns()
        {

        }

        public string childSQL
        {
            get
            {
                //return SQLText("select",whereColumns:;
                return "";
            }
        }


        private string MapDataTypeToTS(string dataType)
        {
            switch (dataType)
            {
                case "String":
                    return "string";
                case "Boolean":
                    return "boolean";
                case "DateTime":
                    return "Date";
                default:
                    return "number";
            }
        }

        private bool isFileExists(string file = "", string path = "")
        {
            if (path == "") path = appDataPath;
            if (file == "")
            {
                file = appSchemaFile;
            }
            else
            {
                file = path + "\\" + file;
            }
            return File.Exists(file);
        }

        public List<ReturnObject> Post(JArray values, JObject args = null)
        {
            // get collection of CommandParam
            List<CommandParam> cmds = GetCommandParamsForPosting(values, args);

            // execute all CommandParams
            List<ReturnObject> retVal = DALData.DAL.Excute(cmds, true);

            return retVal;
        }

        //public ReturnObject Post(JArray values, JObject args = null)
        //{
        //    ReturnObject ret = new ReturnObject();
        //    //if (args != null)
        //    //{
        //    //    DALStamps stamps = new DALStamps(columns, _g.TKVStr(args, _g.KEY_USER_ID));
        //    //}

        //    List<CommandParam> cmds = new List<CommandParam>();

        //    foreach (JObject rec in values)
        //    {
        //        DALTableUpdateParams datCols = new DALTableUpdateParams(this, rec, args);
        //        string sql = SQLText(SQLModes.UPDATE, includeColumns: datCols.fields);
        //        cmds.Add(new CommandParam(sql, datCols.updateParams));
        //    }

        //    //string action = _g.TKVStr(args, _g.KEY_ACTION);
        //    string retVal = DALData.DAL.Excute(cmds);

        //    return ret;
        //}

        public List<CommandParam> GetCommandParamsForPosting(JArray values, JObject args = null)
        {
            // Generate a collection of CommandParam for all records posted for update

            // initialize return CommandParam collection
            List<CommandParam> ret = new List<CommandParam>();

            // clear global error
            clearGlobalError();

            // loop through all the member of JArray values (i.e. rows with changed field values(s))
            foreach (JObject rec in values)
            {
                // identify fields/columns to update and log changes to update tracking table if any.
                DALTableUpdateParams datCols = new DALTableUpdateParams(this, rec, args);

                // error has occured and will result to returning empty list
                if (globalError.Length != 0) return new List<CommandParam>();


                // if any field value(s) have been modified
                if (datCols.fieldsToUpdate != null)
                {
                    // generate CommandParam if fieldsToUpdate collection is not null
                    string sql = SQLText(SQLModes.UPDATE, includeColumns: datCols.fieldsToUpdate);

                    // add the newly created CommandParam to the return collection
                    ret.Add(new CommandParam(sql, datCols.updateParams, this.tableCode, _table: this, _paramKeyValuePosition: datCols.paramKeyValuePosition, _paramKeyValue: datCols.paramKeyValue));

                }

                // add update tracking command params if any
                if (datCols.trackCommandParams != null)
                    ret = _g.MergeCommandParams(ret, datCols.trackCommandParams);

                // add link table command params if any
                if (datCols.linkCommandParams != null)
                    ret = _g.MergeCommandParams(ret, datCols.linkCommandParams);

                // add new record command params if any
                if (datCols.newRecordCommandParam != null)
                    ret = _g.MergeCommandParams(ret, datCols.newRecordCommandParam);

            }

            // return final collection of CommandParam objects
            return ret;
        }

        public ColumnInfo GetColumnByName(string columnName)
        {
            return columns.Find(c => c.name == columnName);
        }

        public ColumnInfo GetColumnByBoolParam(string booleanParameter)
        {
            // this returns stamp fields
            // NOTE! (alv) :2021/01/11 temporarily bypass this function  because this creates duplicate fields
            // in update and insert sql statement .... 
            // 
            return null;
            return columns.Find(c => (bool)c.GetType().GetProperty(booleanParameter).GetValue(c, null));
        }

        string MapFilterFieldnames(string filter, string fieldMap)
        {

            // Mapping fieldname is necessary to make client-side filter expression shorter
            // where original long fieldname is replaced by  a shorter name. before executing
            // get request in the server, shortened field aliases in the filter expression are 
            // scanned and replaced with the proper fieldnames

            if (filter.Length == 0 || fieldMap.Length == 0) return filter;

            // parse field mapping map1,field1;map2,field2,...,map#,field#;

            // fieldname prefixed with this character indicates that nickname is being used
            //const string placeHolder = "{@";
            const string placeHolder = "!$";
            const string placeHolderSubst = "&excl;";

            string ret = filter.Replace(placeHolder, placeHolderSubst);
            string[] mapArr = fieldMap.Split(';');
            foreach (string map in mapArr)
            {
                if (map.Length == 0) continue;
                string[] mArr = map.Split(',');
                ret = ret.Replace(String.Format("{0}{1}|", placeHolderSubst, mArr[0]), String.Format("{0}{1}|", placeHolderSubst, mArr[1]));
            }
            //return ret.Replace(placeHolderSubst,"{");
            return ret.Replace(placeHolderSubst, "");
        }

        public Dictionary<string, dynamic> SQLSelectCommandParam(string fromClauseExpr = "", string selectExpr = "",
            string whereArgs = "", string sortFields = "", bool distinct = false)
        {
            CommandParam ret = new CommandParam();

            DALData.DAL.LogGlobalMessage(fromClauseExpr, "fromClauseExpr");

            Dictionary<string, dynamic> from = SQLFrom(fromClauseExpr);     // get from clause expression
            Dictionary<string, DALTable> tables = from["tables"];

            Dictionary<string, dynamic> selectGroupExpr = SQLSelect(tables, selectExpr, sortFields, distinct);

            string selectClause = selectGroupExpr["select"];                  // get select fields expression
            string fromClause = from["from"];

            string groupByClause = selectGroupExpr["group"];                  // get select fields expression
            string sortClause = selectGroupExpr["sort"];
            JArray lookupParams = selectGroupExpr["lookupParams"];

            Dictionary<string, dynamic> where = SQLWhere(tables, whereArgs.Replace("%", "~"));

            string filterOnLinkCode = where["filterOnLinkCode"];
            bool isFilterOnLink = filterOnLinkCode.Length != 0;

            if (isFilterOnLink)
            {
                LinkedFilter lnkFlt = new LinkedFilter(filterOnLinkCode, this);

                // modify select clause to include linked filter field value
                selectClause = lnkFlt.GetFilterSelectClause(selectClause);

                // modify from clause to set inner join to linked table
                fromClause = lnkFlt.GetFilterFromClause(fromClause);
            }

            string whereClause = where["where"].Length != 0 ? " where " + where["where"] : "";

            DALData.DAL.LogGlobalMessage(tables.Count, "TableCount");
            DALData.DAL.LogGlobalMessage(String.Join(",", tables.Keys.ToArray()), "tableKeys");

            DALData.DAL.LogGlobalMessage(selectClause, "select");
            DALData.DAL.LogGlobalMessage(fromClause, "from");
            DALData.DAL.LogGlobalMessage(whereClause, "where");

            string cmdText = String.Format("{0}{1}{2}{3}{4};", selectClause, fromClause, whereClause, groupByClause, sortClause);
            Dictionary<string, dynamic> cmdParams = where["params"];

            DALData.DAL.LogGlobalMessage(cmdText, "SQL");

            Dictionary<string, dynamic> retVal = new Dictionary<string, dynamic>();
            retVal.Add("command", new CommandParam(cmdText, cmdParams));
            retVal.Add("lookupParams", lookupParams);

            return retVal;
        }

        private Dictionary<string, dynamic> SQLFrom(string fromClauseExpr)
        {
            /* fromClauseExpr :
             * This is the expression after the pipe(|) character separator to the 'table' o=request parameter 
             * 
             * eg: -tre,AN_ASSET_ID,TRE_DAT_TAG;-node,AN_ASSET_ID,REC_TAG;`lkp@sta,AN_STATUS,LKP_ID;`lkp@ocls,AN_ORIG_CLASS,LKP_ID;`lkp@ccls,AN_CURR_CLASS,LKP_ID
             * 
             * Parameters:
             * <joinType><tableCode>,<localField>,<foreignField>[,reverseLink];
             * 
             * joinType: hyphen [-] is code for INNER JOIN, back-tick [`] is code for LEFT JOIN, caret [^] is code to indicate linked table
             * tableCode: table code defined in APP_DATA/schema/config/config.table.<tableCode.json file
             * localField: field definition found in master/primary table
             * foreignField: field definition found in slave/secondary table
             * semi colon [;] - used as join separator
             * 
             * If join type is a caret [^] (NOTE: as of 2020/07/21, still not yet decided if this will be implemented here or in the filter clause):
             * reverseLink - [0|1] switch to indicate if master and slave table code must be reversed in order to resolve the proper link table characteristics. 
             *  Default value = 1.
             * 
             * link table convention:
             * name: lnk_<master tableCode>_<slave tableCode>
             * fields:
             *    <master tableCode>_<slave tableCode>_ida - master table key id
             *    <master tableCode>_<slave tableCode>_idb - slave table key id
             *    
             * link table join example: ft|^an,AN_ID,FT_ID,1;
             * Resulting FROM clause: 
             *      FROM tbl_FailureThreats AS ft 
             *              INNER JOIN (lnk_an_ft AS an_ft 
             *                     INNER JOIN tbl_Anomalies AS an ON an_ft.an_ft_ida = an.AN_ID) ON ft.FT_ID = an_ft.an_ft_idb
             * 
             * 
             * Sample api call:
             * <protocol>://<domain>[:port]/api/app?table=ft|`lkp@grp,FT_GROUP,LKP_ID;`lkp@typ,FT_TYPE,LKP_ID&includedFields=FT_ID`FT_CODE`FT_NAME`FT_GROUP`FT_TYPE`grp.LKP_DESC_B@FTG`typ.LKP_DESC_B@FTT
             * 
             * Yield:
             * select ft.FT_ID, ft.FT_CODE, ft.FT_NAME, ft.FT_GROUP, ft.FT_TYPE, grp.LKP_DESC_B as FTG, typ.LKP_DESC_B as FTT from ((tbl_FailureThreats as [ft] LEFT JOIN sys_Lookups as grp ON ft.FT_GROUP = grp.LKP_ID) LEFT JOIN sys_Lookups as typ ON ft.FT_TYPE = typ.LKP_ID);
             * 
             * 
             */
            // returns from and tables collection
            Dictionary<string, DALTable> tables = new Dictionary<string, DALTable>();
            tables.Add(tableCode, this);    // add the main table in the dictionary

            // Start with from expression with the main table
            // string fromExpr = String.Format("{0} as [{1}]", this.tableName, tableCode);
            string fromExpr = String.Format("{0}{1}{2}",
                this.tableName, TABLE_ALIAS_LINK, tableCode);

            if (fromClauseExpr.Length > 0)
            {
                // build join expression 
                // split into individual join expressions
                string[] joinArr = fromClauseExpr.Split(SQLJoinChars.JOIN_SEPARATOR);
                foreach (string j in joinArr)
                {
                    // check if j is empty
                    if (j.TrimStart(' ').TrimEnd(' ') == "") continue;  // do not process empty argument

                    // initially set parent alias as tableCode of the parent
                    string parAlias = tableCode;

                    // determine join type of the left most join expression, 
                    // the rest of join expressions are considered left join expressions
                    char join = Convert.ToChar(j.Substring(0, 1));

                    // split all joins in optionally cascaded left join links
                    List<string> subJoinArr = j.Substring(1).Split(SQLJoinChars.LEFT_JOIN_SYMBOL).ToList();

                    // if a join has sub joins
                    foreach (string sj in subJoinArr)
                    {
                        // get link expression components
                        string[] jArr = sj.Split(SQLJoinChars.ARGUMENTS_SEPARATOR);

                        // get table-alias components
                        string[] codeArr = jArr[0].Split(SQLJoinChars.ALIAS_SEPARATOR);

                        // get table code
                        string tblCode = codeArr[0];

                        // get table object from tableCollection 
                        DALTable joinTable = tableCollection[tblCode];

                        // if alias is not specified, it is assumed to be the tableCode of the linked table
                        string tblAlias = (codeArr.Length > 1 ? codeArr[1] : tblCode);

                        // add link table to the tables collection
                        tables.Add(tblAlias, joinTable);

                        // get specified local field from the parent table
                        string localField = jArr[1];

                        // if foreignField is not specified, it is assumed to be the key field of the linked table
                        string foreignField = jArr.Length > 2 ? jArr[2] : joinTable.keyCol.name;

                        //string joinFormat = "({0} {1} JOIN {2} as {3} ON {4}.{5} = {6}.{7})";
                        string joinFormat = "({0} {1} JOIN {2}{3}{4} ON {5}.{6} = {7}.{8})";
                        /* 0 - current expression value
                         * 1 - join type INNER or LEFT
                         * 
                         * 2 - linked table name
                         * 3 - table link expression " as " or " "
                         * 4 - linked table alias
                         * 
                         * 5 - parent table alias
                         * 6 - localField from the parent table
                         * 
                         * 7 - linked table alias
                         * 8 - foreignField from the linked table
                         */

                        // creat new from expression ....
                        fromExpr = String.Format(joinFormat,
                            fromExpr,
                            (join == SQLJoinChars.INNER_JOIN_SYMBOL ? "INNER" : "LEFT"),
                            joinTable.tableName,
                            TABLE_ALIAS_LINK,
                            tblAlias,
                            parAlias,
                            localField,
                            tblAlias,
                            foreignField);

                        // assing current link alias as parent alias to be used in the next link expression
                        // parAlias = tblCode;
                        parAlias = tblAlias;

                        // set join type to left for subsequent join expression
                        join = SQLJoinChars.LEFT_JOIN_SYMBOL;
                    }
                }
            }

            Dictionary<string, dynamic> retVal = new Dictionary<string, dynamic>();
            retVal.Add("tables", tables);
            retVal.Add("from", fromExpr.Length != 0 ? " from " + fromExpr : "");

            return retVal;
        }

        private Dictionary<string, dynamic> SQLSelect(Dictionary<string, DALTable> tables, string selectClauseCode = "", string orderByClauseCode = "", bool distinct = false)
        {


            string selectExpr = "";
            string sortExpr = SQLOrder(tables, orderByClauseCode);
            string groupExpr = sortExpr.Replace(" desc", "").Replace(" asc", "");
            string groupExprTmp = groupExpr;
            JArray lkpPrm = null;


            bool withAggregate = false;

            string[] selectGroupByArr = selectClauseCode.Split(SQLJoinChars.PIPE_SEPARATOR);

            DALTable tbl;
            if (selectClauseCode.Length == 0)
            {
                // select all fields from the first table in the dictionary
                tbl = tables.ElementAt(0).Value;
                selectExpr = String.Format("{0}.*", tables.ElementAt(0).Key);
            }
            else
            {
                string[] selectFields = selectGroupByArr[0].Split(SQLJoinChars.FIELD_SEPARATOR);
                selectExpr = "";

                foreach (string field in selectFields)
                {
                    Dictionary<string, dynamic> SQLSelectObj = SQLFieldExpressionObj(tables, field, distinct);

                    if (SQLSelectObj["displayField"].Length != 0)
                    {
                        // lookup definition is required
                        if (lkpPrm == null) lkpPrm = new JArray();

                        string valueField = SQLSelectObj["fieldAlias"].Length != 0 ? SQLSelectObj["fieldAlias"] : SQLSelectObj["field"];
                        string displayField = SQLSelectObj["displayField"];
                        string displayFieldSub = SQLSelectObj["displayFieldSub"];

                        lkpPrm.Add(new JObject()
                        {
                            ["valueField"] = valueField,
                            ["displayField"] = displayField,
                            ["displayFieldSub"] = displayFieldSub,
                            ["lookup"] = new JObject(),

                            ["field"] = SQLSelectObj["field"],
                            ["tableAlias"] = SQLSelectObj["tableAlias"]

                        });

                    }


                    if (SQLSelectObj["aggregate"].Length != 0)
                        withAggregate = true; // set aggregate flag
                    else
                        // Build temporary Group By clause if the field is not yet in the initial groupExpr value (from sort)
                        if (SQLSelectObj["field"] != "*" && groupExprTmp.IndexOf(field) == -1)
                    {
                        groupExprTmp += (groupExprTmp.Length != 0 ? ", " : "") +
                            String.Format("{0}.{1}", SQLSelectObj["tableAlias"], SQLSelectObj["field"]);
                    }

                    // Build select expression
                    selectExpr += (selectExpr.Length != 0 ? ", " : "") + SQLSelectObj["selectExpression"];
                }
            }

            if (withAggregate)
            {
                if (selectGroupByArr.Length == 2)
                {
                    // <select>|<group by> expressions specified
                    string[] groupExprArr = selectGroupByArr[1].Split(SQLJoinChars.ARGUMENTS_SEPARATOR);
                    foreach (string grp in groupExprArr)
                    {
                        if (grp.Length == 0 || groupExpr.IndexOf(grp) != -1) continue;  // grp is empty or if it is already in groupExpr (from sort)

                        if (grp.IndexOf(SQLJoinChars.ALIAS_EXPRESSION) != -1)
                            // alias already specified
                            groupExpr += (groupExpr.Length != 0 ? ", " : "") + grp;
                        else
                            // alias not specified, therefore look for the table where the field is a member of and
                            // use its alias as the table alias of the field
                            foreach (string tblKey in tables.Keys)
                            {
                                ColumnInfo col = tables[tblKey].columns.Find(c => c.name == grp);
                                if (col != null)
                                {
                                    groupExpr += (groupExpr.Length != 0 ? ", " : "") + String.Format("{0}.{1}", tblKey, grp);
                                    break;
                                }
                            }
                    }
                }
                else
                {
                    // Group By parameter not specified, therefore use the regular selected fields as group by expression
                    // if groupExprTmp is empty, it only means that the intention is to get the result of aggregate function(s)
                    // for the entire unfiltered/filtered table
                    groupExpr = groupExprTmp;
                }
            }



            Dictionary<string, dynamic> retVal = new Dictionary<string, dynamic>();
            retVal.Add("select", "select " + (distinct ? "distinct " : "") + selectExpr);
            retVal.Add("group", withAggregate && (groupExpr.Length != 0) ? " group by " + groupExpr : "");
            retVal.Add("sort", sortExpr.Length != 0 ? " order by " + sortExpr : "");
            retVal.Add("lookupParams", lkpPrm);


            return retVal;
        }

        Dictionary<string, dynamic> SQLFieldExpressionObj(Dictionary<string, DALTable> tables, string fieldName, bool distinct = false)
        {
            /* Expected format [tableAlias.]<fieldName>[@[fieldAlias][^displayField][^fieldFormat]]
             * Sample expression:
             *   an.AN_RAISED_DATE@^^DATE
             *   
             * fieldFormat is currently only accepting DATE which forms output 
             * expression yyyy-mm-dd
             */

            Dictionary<string, dynamic> retVal = new Dictionary<string, dynamic>();

            string field = "";
            string fieldExpr = "";
            string fieldAlias = "";
            string tableAlias = "";
            string aggregate = "";


            // split by "@"
            string[] fldArr = fieldName.Split(SQLJoinChars.ALIAS_SEPARATOR);
            // split by "^"
            string fldDisplayField = "";
            string fldSubDisplayFields = "";
            string fldFormat = "";

            //string[] fldArr = fieldName.Split(SQLJoinChars.ALIAS_SEPARATOR);

            if (fldArr.Length == 2)
            {
                // <field expression>@<field alias>
                // <field alias> = fieldAlias[^fldDisplayField[^fldFormat]
                fieldExpr = fldArr[0];
                string[] fldAliasArr = fldArr[1].Split(SQLJoinChars.CARET_SEPARATOR);
                fieldAlias = fldAliasArr[0];
                if (fldAliasArr.Length > 1)
                {
                    int fldMarker = fldAliasArr[1].IndexOf(SQLJoinChars.ARGUMENTS_SEPARATOR);
                    fldDisplayField = (fldMarker == -1 ? fldAliasArr[1] : fldAliasArr[1].Substring(0, fldMarker));
                    if (fldMarker == -1)
                    {
                        fldDisplayField = fldAliasArr[1];
                    }
                    else
                    {
                        fldDisplayField = fldAliasArr[1].Substring(0, fldMarker);
                        fldSubDisplayFields = fldAliasArr[1].Substring(fldMarker + 1);
                    }
                }
                if (fldAliasArr.Length > 2) fldFormat = fldAliasArr[2];
            }
            else if (fldArr.Length == 1)
            {
                fieldExpr = fldArr[0];
            }
            else
            {
                // error in expression ???
                // more than 2 expression components separated by "." ?????
            }

            // process fieldExpr, check if regular fieldname or an aggregate expression
            //
            int aggMark = fieldExpr.IndexOf(SQLJoinCStr.AGGREGATE_SEPARATOR);

            if (aggMark == -1)
            {
                // non-aggregate expression
                field = (fieldExpr == SQLJoinCStr.TILDE_SEPARATOR ? "*" : fieldExpr);
            }
            else
            {
                // aggregate expression <aggregate function>([TableAlias.]<FieldName>)[@FieldAlias]
                aggregate = fieldExpr.Substring(0, aggMark);
                field = fieldExpr.Substring(aggMark + 1, fieldExpr.Length - 1 - aggMark - 1);
            }

            if (field.Length != 0)
            {
                // look for table 
                string[] fieldArr = field.Split(SQLJoinChars.ALIAS_EXPRESSION);
                if (fieldArr.Length == 2)
                {
                    tableAlias = fieldArr[0];
                    field = (fieldArr[1] == SQLJoinCStr.TILDE_SEPARATOR ? "*" : fieldArr[1]);                                // reassign field name without table alias                }
                }

                if (tableAlias.Length == 0)
                {
                    // table alias not specified
                    // search for table alias from all the tables
                    foreach (string tblKey in tables.Keys)
                    {
                        DALTable tbl = tables[tblKey];
                        if (field == "*")
                        {
                            retVal.Add("column", null);
                            tableAlias = tblKey;
                            break;
                        }
                        else
                        {
                            ColumnInfo col = tbl.columns.Find(c => c.name == field);
                            if (col != null)
                            {
                                retVal.Add("column", col);
                                tableAlias = tblKey;
                                break;  // break loop because the field has already been found in one of the tables...
                            }

                        }
                    }
                }
                else
                {
                    // if table alias is set at the argument, find the column object of the field....
                    retVal.Add("column", tables[tableAlias].columns.Find(c => c.name == field));
                }
            }
            else
            {
                // field name not found in the fieldExpr parameter! ????? Errors must have occured
            }

            // form actual field expression/alias
            // for date type column:
            // T-SQL : SELECT FORMAT( <DATE_FIELD>, 'yyyy-MM-dd', 'en-US' ) AS '<FIELD_ALIAS>'
            // JetEngine :  SELECT FORMAT(<DATE_FIELD>, 'yyyy-mm-dd') AS [<FIELD_ALIAS>]
            // Oracle : SELECT TO_CHAR(<DATE_FIELD>, 'yyyy-mm-dd') AS [<FIELD_ALIAS>]

            string DAL_TYPE = DALData.DAL.DAL_TYPE;

            string selectExpression;
            if (aggregate.Length == 0)
            {
                // no aggregate function

                if (fldFormat == "DATE")
                {
                    //DATE_STRING_FORMAT
                    //AN_RAISED_DATE
                    selectExpression = String.Format(DATE_STRING_FORMAT, tableAlias, field) + FIELD_ALIAS_LINK + (fieldAlias.Length != 0 ? fieldAlias : field);

                }
                else
                {
                    selectExpression = String.Format("{0}.{1}", tableAlias, field) + (fieldAlias.Length != 0 ? FIELD_ALIAS_LINK + fieldAlias : "");
                }
            }
            else
            {
                // if aggregate field does not have an alias
                if (fieldAlias.Length == 0) fieldAlias = String.Format("{0}_of_{1}_{2}", aggregate, tableAlias, field);

                // form select field expression
                selectExpression = String.Format("{0}({1}.{2}) as {3}", aggregate, tableAlias, field, fieldAlias);
            }

            retVal.Add("field", field);
            retVal.Add("fieldExpr", fieldExpr);
            retVal.Add("fieldAlias", fieldAlias);
            retVal.Add("tableAlias", tableAlias);
            retVal.Add("displayField", fldDisplayField);
            retVal.Add("displayFieldSub", fldSubDisplayFields);
            retVal.Add("aggregate", aggregate);
            retVal.Add("selectExpression", selectExpression);

            return retVal;

        }

        private string SQLWhereOperator(ColumnInfo col, string values)
        {
            string[] valArr;
            if (values.IndexOf("^") != -1) return "btw";
            if (col.type == "String")
            {
                valArr = SQLSplitStringValues(values);
                return valArr.Length != 1 ? "in" : "eq";
            }
            else if (col.type == "DateTime")
            {

            }
            else
            {
                valArr = values.Split(SQLJoinChars.ARGUMENTS_SEPARATOR);
                return valArr.Length != 1 ? "in" : "eq";
            }
            return "";
        }

        private string SQLActualOperator(string operatorCode)
        {
            switch (operatorCode)
            {
                case "eq": return "=";
                case "neq": return "<>";
                case "lt": return "<";
                //case "or": return "Or";
                //case "and": return "And";
                case "lte": return "<=";
                case "gt": return ">";
                case "gte": return ">=";
                case "btw": return "Between";
                case "nbtw": return "Not Between";
                case "in": return "In";
                case "nin": return "Not In";
                case "lk": return SQL_LIKE;
                case "nlk": return "Not " + SQL_LIKE;
                default:
                    return "=";
            }
        }

        private string[] SQLSplitStringValues(string stringValues)
        {
            return stringValues.TrimEnd('\"').TrimStart('\"')
                        .Replace("\",\"", SQLJoinCStr.CARET_SEPARATOR)
                        .Split(SQLJoinChars.CARET_SEPARATOR);
        }

        private string FieldTemplate(string directive = "")
        {
            return (directive == "DATE" ? DATE_VALUE_CALL : "{0}.{1}");
        }

        private Dictionary<string, dynamic> SQLWhereExpression(string linkCode, string values, int paramsIndex, Dictionary<string, dynamic> prms, string directive = "")
        {

            // when the current table is the child in the link table definition
            bool isReverseLink = linkCode.StartsWith(SQLJoinCStr.LINK_LEFT_FILTER_SYMBOL);
            string linkTableCode = linkCode.Substring(1);

            // Form {fieldAlias}.{fieldName} string template
            string tplFieldAliasFieldName = FieldTemplate(directive);

            //string xFix = String.Format("{0}_{1}", isReverseLink ? linkTableCode : this.tableCode, isReverseLink ? this.tableCode: linkTableCode);
            //string linkTableName = String.Format("lnk_{0}", xFix);
            //string linkFilterField = String.Format(xFix + "_id{0}", isReverseLink ? "b" : "a");

            LinkedFilter lnkFlt = new LinkedFilter(linkCode, this);

            string[] valArr = values.Split(SQLJoinChars.ARGUMENTS_SEPARATOR);

            string prmKey = String.Format(PARAM_PREFIX + "p{0}", paramsIndex);
            string valsParams = "";
            string expr = "";

            if (valArr.Length == 0)
                ; // no value(s) specified
            else if (valArr.Length == 1 && valArr[0].Length != 0)
            {
                prms.Add(prmKey, Convert.ToDouble(valArr[0]));
                expr = String.Format(tplFieldAliasFieldName + " = {2}", lnkFlt.xFix, lnkFlt.linkFilterField, prmKey);
            }
            else
            {
                foreach (string val in valArr)
                {
                    if (val.Length == 0) continue;

                    prmKey = String.Format(PARAM_PREFIX + "p{0}", paramsIndex);

                    valsParams += (valsParams.Length != 0 ? ", " : "") + prmKey;
                    prms.Add(prmKey, Convert.ToDouble(val));
                    paramsIndex++;
                }
                if (valsParams.Length != 0) expr = String.Format("(" + tplFieldAliasFieldName + " In ({2}))", lnkFlt.xFix, lnkFlt.linkFilterField, valsParams);
            }

            Dictionary<string, dynamic> ret = new Dictionary<string, dynamic>();

            ret.Add("where", expr);
            ret.Add("paramsIndex", paramsIndex);
            ret.Add("params", prms);

            return ret;
        }

        private Dictionary<string, dynamic> SQLWhereExpression(ColumnInfo col, string tableAlias, string field, string optr, string values, int paramsIndex, Dictionary<string, dynamic> prms, string directive = "")
        {

            Dictionary<string, dynamic> ret = new Dictionary<string, dynamic>();
            string expr = "";

            // Form {fieldAlias}.{fieldName} string template
            string tplFieldAliasFieldName = FieldTemplate(directive);

            if (values == "null")
            {
                // handle null values

                expr = String.Format("(" + tplFieldAliasFieldName + " {2})", tableAlias, field, optr == "eq" ? "Is Null" : "Is Not Null");

                ret.Add("where", expr);
                ret.Add("paramsIndex", paramsIndex);
                ret.Add("params", prms);

                return ret;
            }

            string[] valArr = new string[] { };
            string type = col.type;

            bool isString = (type == "String");
            bool isDate = (type == "DateTime");

            if (optr == "in" || optr == "nin")
                // in or not in
                if (isString)
                {
                    valArr = SQLSplitStringValues(values);
                }
                else
                {
                    // not required to have enclosing double quotes
                    valArr = values.Split(SQLJoinChars.ARGUMENTS_SEPARATOR);
                }
            else if (optr == "btw" || optr == "nbtw")
                // <table>.<field>|<btw|nbtw>|<values>
                if (isString)
                {
                    valArr = values.TrimEnd('\"').TrimStart('\"')
                        .Replace("\"" + SQLJoinCStr.CARET_SEPARATOR + "\"", SQLJoinCStr.CARET_SEPARATOR)
                        .Split(SQLJoinChars.CARET_SEPARATOR);
                }
                else
                {
                    // not required to have enclosing double quotes
                    valArr = values.Split(SQLJoinChars.CARET_SEPARATOR);
                }

            string SQLOptr = SQLActualOperator(optr);
            string valsParams = "";
            string prmKey = String.Format(PARAM_PREFIX + "p{0}", paramsIndex);
            switch (optr)
            {
                case "lk":
                case "nlk":
                    expr = String.Format(tplFieldAliasFieldName + " {2} {3}", tableAlias, field, SQLOptr, prmKey);
                    prms.Add(prmKey, values.TrimStart('\"').TrimEnd('\"').Replace("~", "%"));
                    paramsIndex++;

                    break;
                case "lt":
                case "lte":
                case "gt":
                case "gte":
                case "eq":
                case "neq":
                    if (isString)
                    {
                        if ((optr == "eq" || optr == "neq") &&
                            (values.EndsWith("~\"") || values.StartsWith("\"~") || values.EndsWith("_\"") || values.StartsWith("\"_")))
                            SQLOptr = optr == "eq" ? SQL_LIKE : "Not " + SQL_LIKE;

                        prms.Add(prmKey, values.TrimStart('\"').TrimEnd('\"').Replace("~", "%"));

                    }
                    else
                        prms.Add(prmKey, ToNumberOrDate(values, isDate));

                    expr = String.Format(tplFieldAliasFieldName + " {2} {3}", tableAlias, field, SQLOptr, prmKey);
                    break;

                case "nin":
                case "in":
                    if (isString && (values.IndexOf("~") != -1 || values.IndexOf("_\"") != -1 || values.IndexOf("\"_") != -1))
                    {
                        // string expression has wild characters
                        foreach (string val in valArr)
                        {
                            string notExpr = (optr == "nin" ? "Not " : "");
                            string eqExpr = (optr == "nin" ? "<>" : "=");
                            string logExpr = (optr == "nin" ? " And " : " Or ");

                            prmKey = String.Format(PARAM_PREFIX + "p{0}", paramsIndex);

                            if (val.EndsWith("_") || val.EndsWith("~") || val.StartsWith("_") || val.StartsWith("~"))
                            {
                                // with wild characters
                                expr += (expr.Length != 0 ? logExpr : "") + String.Format("(" + tplFieldAliasFieldName + " {2}" + SQL_LIKE + " {3})", tableAlias, field, notExpr, prmKey);
                            }
                            else
                            {
                                expr += (expr.Length != 0 ? logExpr : "") + String.Format("(" + tplFieldAliasFieldName + " {2} {3})", tableAlias, field, eqExpr, prmKey);
                            }
                            prms.Add(prmKey, val.Replace("~", "%"));
                            paramsIndex++;
                        }

                        if (expr.Length != 0) expr = "(" + expr + ")";

                    }
                    else
                    {
                        // non-string in expression ...
                        foreach (string val in valArr)
                        {
                            prmKey = String.Format(PARAM_PREFIX + "p{0}", paramsIndex);

                            valsParams += (valsParams.Length != 0 ? ", " : "") + prmKey;
                            if (isString)
                                prms.Add(prmKey, val);
                            else
                                prms.Add(prmKey, ToNumberOrDate(val, isDate));
                            paramsIndex++;
                        }
                        expr = String.Format("(" + tplFieldAliasFieldName + " {2} ({3}))", tableAlias, field, SQLOptr, valsParams);
                    }
                    break;

                case "btw":
                case "nbtw":
                    if (isString)
                    {
                        prms.Add(String.Format(PARAM_PREFIX + "p{0}", paramsIndex), valArr[0]);
                        prms.Add(String.Format(PARAM_PREFIX + "p{0}", paramsIndex + 1), valArr[1]);
                    }
                    else
                    {
                        //prms.Add(String.Format("@p{0}", paramsIndex), Convert.ToDouble(valArr[0]));
                        //prms.Add(String.Format("@p{0}", paramsIndex + 1), Convert.ToDouble(valArr[1]));
                        prms.Add(String.Format(PARAM_PREFIX + "p{0}", paramsIndex), ToNumberOrDate(valArr[0], isDate));
                        prms.Add(String.Format(PARAM_PREFIX + "p{0}", paramsIndex + 1), ToNumberOrDate(valArr[1], isDate));
                    }

                    expr = String.Format("(" + tplFieldAliasFieldName + " {2} " + PARAM_PREFIX + "p{3} And " + PARAM_PREFIX + "p{4})", tableAlias, field, SQLOptr, paramsIndex, paramsIndex + 1);
                    paramsIndex++;
                    break;
            }

            ret.Add("where", expr);
            ret.Add("paramsIndex", paramsIndex);
            ret.Add("params", prms);

            return ret;
        }

        private dynamic ToNumberOrDate(string val, bool isDate = false)
        {
            if (val == "null") return DBNull.Value;

            if (isDate)
            {
                if (val == null) return null;
                if (val.Length == 0) return null;
                // if string type date, return value as string
                if (val.IndexOf("\"") != -1) return val;
                return Convert.ToDateTime(val);
            }
            else
            {
                return Convert.ToDouble(val);
            }
        }

        private Dictionary<string, dynamic> SQLWhere(Dictionary<string, DALTable> tables, string whereArgs = "")
        {
            // Builds parametric where clause and parameters dictionary
            // parses whereArgs to extract and replace all expressions inside pairs of curly brackets
            // {<field>|[operator]|<value(s)>} 
            // if operator is not existing, it is set to 'eq' or 'in'(if second parameter is comma delimitted values)
            // operators eq,neq,lt,lte,gt,gte,in,nin,btw,nbtw
            // ^ is And logical operator | is or logical operator
            string args = whereArgs;
            int expIdx = 0;
            int expMarker = args.IndexOf(SQLJoinChars.WHERE_START_SEPARATOR);
            int paramsIndex = 0;

            Dictionary<string, dynamic> prms = new Dictionary<string, dynamic>();
            Dictionary<string, string> expressions = new Dictionary<string, string>();
            bool isFilterOnLink = false;
            string filterOnLinkCode = "";
            string fieldDirective = "";

            while (expMarker != -1)
            {
                string expKey = String.Format("[EXPR{0}]", expIdx);
                int endIdx = args.IndexOf(SQLJoinChars.WHERE_END_SEPARATOR);

                // extract just the characters inside the { } brackets
                string fieldExpr = args.Substring(expMarker + 1, endIdx - expMarker - 1);

                isFilterOnLink = (fieldExpr.StartsWith(SQLJoinCStr.LINK_LEFT_FILTER_SYMBOL) || fieldExpr.StartsWith(SQLJoinCStr.LINK_RIGHT_FILTER_SYMBOL));

                // prcocess expression here
                string[] fieldExprArr = fieldExpr.Split(SQLJoinChars.OPERATOR_PIPE);
                string[] fieldArr = null;
                string field = "";
                string tblAlias = "";
                string optr = "";
                string values = "";

                // to check if alias is included as part of the fieldName
                if (fieldExprArr.Length > 1) fieldArr = fieldExprArr[0].Split(SQLJoinChars.ALIAS_EXPRESSION);

                if (fieldExprArr.Length == 1 && !isFilterOnLink)
                    // no fieldName specified and only value(s) component is supplied. 
                    // this means that filter will be applied to the key field of the table
                    // using either eq or in operator
                    values = fieldExprArr[0];
                else if (fieldExprArr.Length == 1 && isFilterOnLink)
                    // field indicates that link filtering is required but with no filter value(s)
                    field = fieldExprArr[0];
                else if (fieldExprArr.Length == 2)
                {
                    // only field and value(s) compenents are supplied. 
                    // filter will be applied to the specified field
                    // using either eq or in operator
                    if (fieldArr.Length == 2)
                    {
                        tblAlias = fieldArr[0];
                        field = fieldArr[1];
                    }
                    else
                        field = fieldExprArr[0];

                    values = fieldExprArr[1];
                }
                else if (fieldExprArr.Length == 3)
                {
                    // all three expressions are supplied (i.e. fieldname, operator and value(s))
                    if (fieldArr.Length == 2)
                    {
                        tblAlias = fieldArr[0];
                        field = fieldArr[1];
                    }
                    else
                        field = fieldExprArr[0];

                    optr = fieldExprArr[1];
                    values = fieldExprArr[2];
                }

                KeyValuePair<string, DALTable> tblItem;
                DALTable tbl;
                ColumnInfo col = null;

                if (field.Length == 0)
                {
                    // no field is specified, therefore assume the field as the key field of the table
                    tblItem = tables.ElementAt(0);

                    tbl = tblItem.Value;
                    tblAlias = tblItem.Key;
                    col = tbl.keyCol;

                    field = col.name;

                }
                else
                {
                    //if(field.StartsWith(SQLJoinCStr.))
                    // field is specified, check if table alias is available to eliminate iteration through the table collection

                    // check if field includes directive required to determine 
                    // if a function using the field as parameter is required
                    // to evaluate conditional expression
                    // ie. field = [tableAlias.]<fieldName[$<directive>]>

                    /* directives:
                     * DATE : use DateValue(field) function for Jet, to_date(field) for Oracle
                    */

                    string[] fieldNameArr = field.Split('$');
                    field = fieldNameArr[0];    // reassign fieldname
                    if (fieldNameArr.Length > 1) fieldDirective = fieldNameArr[1];


                    if (isFilterOnLink)
                        filterOnLinkCode = field;
                    else if (tblAlias.Length != 0)
                    {
                        tbl = tables[tblAlias];
                        col = tbl.columns.Find(c => c.name == field);
                    }
                    else
                        // iterate through the table collection to get table alias
                        foreach (string tblKey in tables.Keys)
                        {
                            tbl = tables[tblKey];
                            tblAlias = tblKey;
                            col = tbl.columns.Find(c => c.name == field);

                            if (col != null) break;

                            tbl = null;
                            tblAlias = "";
                        }
                }

                if (optr.Length == 0 && !isFilterOnLink) optr = SQLWhereOperator(col, values);

                DALData.DAL.LogGlobalMessage(tblAlias, "tblAlias");
                DALData.DAL.LogGlobalMessage(field, "field");
                DALData.DAL.LogGlobalMessage(optr, "operator");

                // build new where expression out of the derived tblAlias, field, optr and values
                Dictionary<string, dynamic> finalExpression;
                if (isFilterOnLink)
                    finalExpression = SQLWhereExpression(filterOnLinkCode, values, paramsIndex, prms, fieldDirective);
                else
                    finalExpression = SQLWhereExpression(col, tblAlias, field, optr, values, paramsIndex, prms, fieldDirective);

                paramsIndex = finalExpression["paramsIndex"] + 1;

                // add processed expression in the dictionary
                expressions.Add(expKey, finalExpression["where"]);

                // replace original expression with expKey (expression key)
                args = args.Substring(0, expMarker) + expKey + args.Substring(endIdx + 1);

                // locate new expression
                expMarker = args.IndexOf(SQLJoinChars.WHERE_START_SEPARATOR);
                expIdx++;
            }

            DALData.DAL.LogGlobalMessage(args, "markedArgs");
            DALData.DAL.LogGlobalMessage(expressions.Count, "expressions.count");

            // replace ^-And  and |-Or operators outside the where expressions
            args = args.Replace(SQLJoinCStr.CARET_SEPARATOR, " And ").Replace(SQLJoinCStr.OPERATOR_PIPE, " Or ");
            // replace all expression markers with the processed expression 
            foreach (string key in expressions.Keys)
            {
                args = args.Replace(key, expressions[key]);
            }

            DALData.DAL.LogGlobalMessage(args, "processed expressions");

            Dictionary<string, dynamic> ret = new Dictionary<string, dynamic>();
            ret.Add("params", prms);
            ret.Add("where", args);
            ret.Add("filterOnLinkCode", filterOnLinkCode);

            return ret;
        }


        private string SQLOrder(Dictionary<string, DALTable> tables, string orderClauseCode)
        {
            string orderByExpr = "";
            string[] orderByArr = orderClauseCode.Split(SQLJoinChars.ARGUMENTS_SEPARATOR);
            foreach (string sort in orderByArr)
            {
                if (sort.Length == 0) continue;
                string sortField = sort;
                string sortDir = sort.StartsWith("-") ? " desc" : "";
                if (sortDir.Length != 0) sortField = sortField.Substring(1);

                if (sortField.IndexOf(SQLJoinChars.ALIAS_EXPRESSION) == -1)
                    // table alias not specified, search field from the tables and use the table code as alias
                    foreach (string tblKey in tables.Keys)
                    {
                        ColumnInfo col = tables[tblKey].columns.Find(c => c.name == sortField);
                        if (col != null)
                        {
                            orderByExpr += (orderByExpr.Length != 0 ? ", " : "") + tblKey + "." + sortField + sortDir;
                            break;
                        }
                    }
                else
                    orderByExpr += (orderByExpr.Length != 0 ? ", " : "") + sortField + sortDir;
            }
            return orderByExpr;
        }

        private string SQLAggregates(string aggExprCode)
        {
            return null;
        }


        private JObject ParseRequestConfig(string config = "")
        {
            JObject ret = new JObject();
            if (config.Length == 0) return ret;

            /* requestConfig:
                A. <aggregate>=rel pair -
                    count=tre,first=tre
                B. @<relation>=<aggregate1>|[field1],<aggregate2>|[field2],...,<aggregate#>|[field#]
                   if field# is not supplied, it is assumed that the keyField on the linked table is used in the aggregate
                   results to having agg_<rel>_<aggregate1>,agg_<rel>_<aggregate2>,...,agg_<rel>_<aggregate#> fields in the output
             */

            if (config.StartsWith("@"))
            {

                // B. @<relation>=<aggregate1>|[field1],<aggregate2>|[field2],...,<aggregate#>|[field#]
                // if a group token is existing in the return value then processing will be made
                //ret.Add("group", 1);
                DALData.DAL.LogGlobalMessage(config, "requestConfig");
                string[] cfgArr = config.Split('=');
                string[] cfgArrSub = cfgArr[1].Split(',');
                JArray aggJArr = new JArray();
                foreach (string agg in cfgArrSub)
                {
                    string[] aggArr = agg.Split('|');
                    // add new {function:'<aggregate>', field:[fieldname]} object to aggJArr
                    aggJArr.Add(new JObject() { ["function"] = aggArr[0], ["field"] = aggArr.Length > 1 ? aggArr[1] : "" });
                }
                ret.Add("group", aggJArr);
            }
            else
            {
                string[] cfgArr = config.Split(',');
                foreach (string cfg in cfgArr)
                {
                    string[] cfgValArr = cfg.Split('=');
                    ret.Add(cfgValArr[0], cfgValArr[1]);
                }
            }
            return ret;
        }

        public List<ReturnObject> GetData(JObject args = null, JObject reqParams = null, int objOrder = -1)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            string key = _g.TKVStr(reqParams, "key").TrimEnd(' ').TrimStart(' ');
            string keyField = _g.TKVStr(reqParams, "keyField").TrimEnd(' ').TrimStart(' ');
            string includedFields = _g.TKVStr(reqParams, "includedFields");
            string fieldMap = _g.TKVStr(reqParams, "fieldMap");
            string filter = MapFilterFieldnames(_g.TKVStr(reqParams, "filter"), fieldMap);
            string sortFields = _g.TKVStr(reqParams, "sortFields").TrimEnd(' ').TrimStart(' ');

            long pageNumber = _g.TKV64(reqParams, "pageNumber", 0);
            long pageSize = _g.TKV64(reqParams, "pageSize", 0);
            bool snapshot = _g.TKVBln(reqParams, "snapshot");
            bool distinct = _g.TKVBln(reqParams, "distinct");

            if (key.Length != 0 && filter.Length == 0)
            {
                if (keyField.Length == 0) keyField = this.keyCol.name;

                string expr = "";
                string[] keyValArr = key.Split('`');
                string[] keyFieldArr = keyField.Split('`');
                int maxIndex = Math.Min(keyValArr.Length, keyFieldArr.Length);

                for (int i = 0; i < maxIndex; i++)
                {
                    expr += (expr.Length != 0 ? "^" : "") + "{" + String.Format("{0}|{1}", keyFieldArr[i], keyValArr[i]) + "}";
                }

                filter = expr;
            }

            string fromClauseExpr = _g.TKVStr(reqParams, "fromClauseExpr").TrimEnd(' ').TrimStart(' ');
            string selectExpr = includedFields;
            string whereValues = key;
            string sortExpr = sortFields;
            string whereFields = keyField;

            Dictionary<string, dynamic> sqlSelectCommandObject = SQLSelectCommandParam(fromClauseExpr, selectExpr, filter, sortExpr, distinct);

            CommandParam sqlSelectCommandParam = sqlSelectCommandObject["command"];

            List<ReturnObject> ret = new List<ReturnObject>();

            //DALData.DAL.LogGlobalMessage(sqlSelectCommandParam.cmdText,"SQL");
            //ReturnObject rval = new ReturnObject();
            //rval.returnType = "table";
            //rval.returnCode = tableCode;
            //rval.result.returnDataParams.Add("SQL",sqlSelectCommandParam.cmdText);
            //ret.Add(rval);
            //return ret;

            ReturnObject tbl = DALData.DAL.GetRecordset(sqlSelectCommandParam, withFields: false,
                pageNumber: pageNumber, pageSize: pageSize,
                lookupParams: sqlSelectCommandObject["lookupParams"]);

            // addidional returnDataParams
            tbl.result.returnDataParams.Add("snapshot", snapshot);
            tbl.result.returnDataParams.Add("distinct", distinct);
            tbl.result.returnDataParams.Add("includedFields", includedFields);
            tbl.result.returnDataParams.Add("key", key);
            tbl.result.returnDataParams.Add("keyField", keyField);
            tbl.result.returnDataParams.Add("sortFields", sortFields);

            tbl.returnCode = tableCode;
            tbl.returnType = "table";

            stopwatch.Stop();
            tbl.result.requestDuration = stopwatch.ElapsedMilliseconds;

            ret.Add(tbl);

            return ret;
        }

        public List<ReturnObject> Get(JObject args = null, JObject reqParams = null, int objOrder = -1)
        {
            // will become obsolete once the GetData method is fully implemented 2020/06/20

            //return GetData(args, reqParams, objOrder);

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();


            //List<ReturnObject> ret1 = GetData(args, reqParams, objOrder);
            //return ret1;

            List<ReturnObject> ret = new List<ReturnObject>();

            string key = _g.TKVStr(reqParams, "key").TrimEnd(' ').TrimStart(' ');
            string keyField = _g.TKVStr(reqParams, "keyField").TrimEnd(' ').TrimStart(' ');
            string includedFields = _g.TKVStr(reqParams, "includedFields");
            string filter = _g.TKVStr(reqParams, "filter");
            string fieldMap = _g.TKVStr(reqParams, "fieldMap");
            string sortFields = _g.TKVStr(reqParams, "sortFields").TrimEnd(' ').TrimStart(' ');

            string requestConfig = _g.TKVStr(reqParams, "requestConfig").TrimEnd(' ').TrimStart(' ');

            // split passed table code to separate main table code and fromClause parameters
            string childTableCode = _g.TKVStr(reqParams, "childTableCode").TrimEnd(' ').TrimStart(' ');

            long pageNumber = _g.TKV64(reqParams, "pageNumber", 0);
            long pageSize = _g.TKV64(reqParams, "pageSize", 0);

            bool snapshot = _g.TKVBln(reqParams, "snapshot");
            bool distinct = _g.TKVBln(reqParams, "distinct");


            // if key value is specified and key field is not!
            if (keyField.Length == 0 && key.Length != 0) keyField = this.keyCol.name;

            List<ColumnInfo> keyCols = null;
            List<ColumnInfo> incCols = ColumnsFromIndices(includedFields);
            List<ColumnInfo> sortColumns = ColumnsFromIndices(sortFields.Replace("-", ""));
            List<int> whereValCounts = null;

            string fromClauseExpr = _g.TKVStr(reqParams, "fromClauseExpr").TrimEnd(' ').TrimStart(' ');
            string selectExpr = includedFields;
            string whereValues = key;
            string sortExpr = sortFields;
            string whereFields = keyField;

            bool debugMode = args != null ? (args.ContainsKey("runMode") ? _g.TKVStr(args, "runMode") == "debug" : false) : false;

            DALTable parentTable = null;

            // child table is the current table when childTableCode is not supplied
            // otherwise, child table where records are to be extracted from will be
            // taken from the tableCollection

            DALTable childTable = (childTableCode.Length == 0 ? this : this.tableCollection[childTableCode]);

            string linkToParentCode = "";
            string keyColsStr = "";

            if (keyField.Length != 0)
            {

                string[] keyFieldArr = keyField.Split(_g.PARAMS_DELIM_CHAR);


                keyCols = new List<ColumnInfo>();


                for (int i = 0; i < keyFieldArr.Length; i++)
                {
                    //string[] kfArr = keyFieldArr[i].Split('|');
                    string kf = keyFieldArr[i];
                    ColumnInfo ci;
                    if (kf.IndexOf('@') == 0)
                    {
                        // get parent table code | condition field components from a single field parameter
                        string[] keyFieldCatArr = kf.Substring(1).Split(_g.PARAMS_DELIM_CHAR_CAT);
                        // get parent table code component
                        linkToParentCode = keyFieldCatArr[0];
                        // get parent object from the tables collection
                        parentTable = this.tableCollection[linkToParentCode];
                        // get condition field component
                        keyField = (keyFieldCatArr.Length >= 2 ? keyFieldCatArr[1] : "");

                        // search search column info from the parent table's columns collection
                        ci = keyField.Length != 0 ? parentTable.columns.Find(c => c.name == keyField) : null;

                        //if(ci != null)
                        //{
                        //    keyColsStr += ci.name;
                        //    //keyCols.Add(ci);
                        //}
                        //else
                        //    ;// possibly use the key field of the parent table 2020-05-24????

                    }
                    else
                    {
                        // key field is coming from the child table where main data will be extracted
                        ci = columns.Find(c => c.name == kf);
                    }

                    if (ci != null)
                    {
                        keyColsStr += (keyColsStr.Length != 0 ? _g.PARAMS_DELIM_CHAR.ToString() : "") + ci.name;
                        keyCols.Add(ci);
                    }

                }

                //if (keyField.IndexOf('@') == 0)
                //{
                //    // current table is a child of the parent table whose tableCode is prefixed by an "@" symbol

                //    // key value(s) supplied are key/group value(s) from the parent table
                //    // keyFields formats:
                //    //  [fidx0]`[fidx1]`[fidx2]`[fidx#]
                //    //  [@parentCode]|[fidx0]`[fidx1]`[fidx2]`[fidx#]

                //    string[] keyFieldCatArr = keyField.Substring(1).Split(_g.PARAMS_DELIM_CHAR_CAT);
                //    linkToParentCode = keyFieldCatArr[0];

                //    // get parent table object from the tables collection
                //    parentTable = this.tableCollection[linkToParentCode];

                //    keyField = (keyFieldCatArr.Length >= 2 ? keyFieldCatArr[1] : "");

                //    keyCols = parentTable.ColumnsFromIndices(keyField);

                //}
                //else
                //{
                //    keyCols = ColumnsFromIndices(keyField);

                //}

            }

            // if parent table is existing
            if (parentTable != null)
            {

                // if parentTable is existing, check if the includedFields argument contains
                // fields that belong to the parent table and include them to incCols collection
                if (includedFields.Length != 0)
                {
                    List<ColumnInfo> parIncFields = parentTable.ColumnsFromIndices(includedFields);
                    parIncFields.ForEach(c => incCols.Add(c));
                }
                // and also check if sort fields from parentTable are included 
                if (sortFields.Length != 0)
                {
                    List<ColumnInfo> parSortColumns = parentTable.ColumnsFromIndices(sortFields.Replace("-", ""));
                    parSortColumns.ForEach(c => sortColumns.Add(c));

                }
            }

            // filter parameter passed
            if (key.Length != 0)
            {
                // parameter(s) are passed ...
                whereValCounts = new List<int>();
                string[] keyColVals = key.Split(_g.PARAMS_DELIM_CHAR);
                foreach (string colVals in keyColVals)
                {
                    string[] vals = colVals.Split(_g.PARAMS_VAL_DELIM_CHAR);
                    whereValCounts.Add(vals.Length);
                }
            }

            List<bool> sortDirections = null;
            // sort parameter passed ...
            if (sortFields.Length != 0)
            {
                sortDirections = new List<bool>();
                string[] sFields = sortFields.Split(_g.PARAMS_DELIM_CHAR);
                foreach (string sField in sFields)
                {
                    // true is ascending and false if descending
                    sortDirections.Add(sField.IndexOf("-") == -1);
                }
            }

            //string sql = SQLText(SQLModes.SELECT, noSort: true, noCond: true);
            //string sql2 = SQLText(SQLModes.SELECT,sortColumns: sortColumns, noSort: true, includeColumns: incCols,
            //    noCond: key.Length == 0, whereColumns: keyCols, whereValCounts: whereValCounts);

            string sql2 = SQLText(SQLModes.SELECT, sortColumns: sortColumns,
                sortDirections: sortDirections, includeColumns: incCols,
                noCond: key.Length == 0, whereColumns: keyCols,
                whereValCounts: whereValCounts, keyVals: key,
                parentTable: parentTable, childTable: childTable, requestConfig: requestConfig);

            // build parameter values
            Dictionary<string, dynamic> prms = BuildCommandParamsFromKeys(key, keyField, parentTable: parentTable);

            ReturnObject tbl = DALData.DAL.GetRecordset(new CommandParam(sql2, prms),
                withFields: false, pageNumber: pageNumber, pageSize: pageSize);

            // addidional returnDataParams
            tbl.result.returnDataParams.Add("snapshot", snapshot);
            tbl.result.returnDataParams.Add("distinct", distinct);
            tbl.result.returnDataParams.Add("includedFields", includedFields);
            tbl.result.returnDataParams.Add("key", key);
            tbl.result.returnDataParams.Add("keyField", keyField);
            tbl.result.returnDataParams.Add("sortFields", sortFields);
            tbl.result.returnDataParams.Add("linkToParentCode", linkToParentCode);




            //string testSQL = "SELECT R.* FROM tbl_ReferenceFiles AS R " +
            //    "INNER JOIN lnk_an_rf AS L ON R.RF_ID = L.an_rf_idb " +
            //    "WHERE(((L.an_rf_ida) = 672 Or(L.an_rf_ida) = 671) AND((R.RF_ID) = 21804)) ORDER BY R.RF_ID;";

            //testSQL = "select * from lnk_an_rf;";

            //ReturnObject testTbl = DALData.DAL.GetRecordset(new CommandParam(testSQL), withFields: false);
            //testTbl.returnType = "link";
            //testTbl.returnCode = "lnk_an_rf";   // key is an....

            tbl.returnCode = tableCode;
            tbl.returnType = "table";

            stopwatch.Stop();
            tbl.result.requestDuration = stopwatch.ElapsedMilliseconds;

            ret.Add(tbl);

            // check if linked table records are available

            return ret;
        }


        /***************************************************************************************************
         * SQL Builder Methods
         ***************************************************************************************************/

        public string SQLTextSelectForTracking(List<ColumnInfo> includeColumns = null)
        {
            List<ColumnInfo> whereColumns = new List<ColumnInfo>() { keyCol };
            return SQLText(mode: SQLModes.SELECT, includeColumns: includeColumns, whereColumns: whereColumns);
        }

        public CommandParam CreateChangeTrackCommand(string uid,
            string action,
            string fieldName,
            string keyValue,
            string oldValue, string tableCode = null)
        {
            // if table code is not supplied, use the current table's tableCode
            if (tableCode == null) tableCode = this.tableCode;

            string sql = this.tableChangeTrack.SQLText(SQLModes.INSERT);
            Dictionary<string, dynamic> prmVals = new Dictionary<string, dynamic>();

            // 0 - trk_id
            // 1 - trk_user_login
            // 2 - trk_table_code
            // 3 - trk_field_name
            // 4 - trk_action
            // 5 - trk_key_value
            // 6 - trk_stamp
            // 7 - trk_rec_info

            prmVals.Add(PARAM_PREFIX + "p0", this.tableChangeTrack.NewAutoId);
            prmVals.Add(PARAM_PREFIX + "p1", uid);
            prmVals.Add(PARAM_PREFIX + "p2", tableCode);
            prmVals.Add(PARAM_PREFIX + "p3", fieldName);
            prmVals.Add(PARAM_PREFIX + "p4", action);
            prmVals.Add(PARAM_PREFIX + "p5", keyValue);
            prmVals.Add(PARAM_PREFIX + "p6", _g.DateTimeNow);
            prmVals.Add(PARAM_PREFIX + "p7", oldValue);

            CommandParam ret = new CommandParam(sql, prmVals,
                tableCode + "_" + this.tableChangeTrack.tableCode, _table: this.tableChangeTrack);
            return ret;
        }


        public string SQLText(
            string mode = SQLModes.FIELDS,
            int ctr = 0,
            List<ColumnInfo> includeColumns = null,
            List<ColumnInfo> whereColumns = null,
            List<ColumnInfo> sortColumns = null,
            List<int> whereValCounts = null,        // count of values specified per field to be 
            List<bool> sortDirections = null,        // sort directions
            bool noCond = false,
            bool noSort = false,
            bool byGroup = false,
            bool fromInsert = false,
            DALTable parentTable = null,
            DALTable childTable = null,
            string parentField = null,
            string requestConfig = "",
            JObject jRequestConfig = null,
            string keyVals = null)
        {
            // parentField - field from the parent table where the key field of the child 
            //               table is goiing to be linked to

            List<ColumnInfo> cols = columns;
            int initCtr = ctr;
            bool isInitial = false;
            string ret = "";
            //string fmt = "[{0}]";
            string fmt = "{0}";

            bool condOrDel = (mode == SQLModes.CONDITION || mode == SQLModes.DELETE);
            bool fromLink = (parentTable != null && childTable != null);
            DALRelation rel = null;
            DALRelation relCount = null;
            DALRelation relFirst = null;
            DALRelation relMax = null;
            bool isWithGroupAggregate = false;

            List<string> whereKeyVals = null;
            if (keyVals != null)
            {
                string[] keyValArr = keyVals.Split(_g.PARAMS_DELIM_CHAR);

                whereKeyVals = new List<string>();
                foreach (string vvv in keyValArr) whereKeyVals.Add(vvv);
            }

            // if requestConfig contains a value and jRequestConfig is not yet set (first time call)
            if (requestConfig.Length != 0 && jRequestConfig == null)
            {
                jRequestConfig = ParseRequestConfig(requestConfig);
            }
            else if (jRequestConfig != null)
            {
                isWithGroupAggregate = jRequestConfig.ContainsKey("group");
            }
            if (!DALData.DAL.isKeyExistInGlobalMessage("isWithGroupAggregate"))
                DALData.DAL.LogGlobalMessage(isWithGroupAggregate, "isWithGroupAggregate");

            string countRelCode = "";
            string firstRelCode = "";
            string maxRelCode = "";

            string countRelField = "";
            string firstRelField = "";
            string maxRelField = "";

            bool withFirstAgreggate = false; //(firstRelCode.Length != 0 && rel.type == TableRelationTypes.ONE2MANY);
            bool withCountAgreggate = false; //(countRelCode.Length != 0 && rel.type == TableRelationTypes.ONE2MANY);
            bool withMaxAgreggate = false;

            // relation is coming from the parent table but extraction of records will be made from the
            // child table (main table)
            if (fromLink) rel = parentTable.tableRelations[childTable.tableCode];


            if (jRequestConfig != null)
            {
                if (!jRequestConfig.ContainsKey("group"))
                {
                    string[] firstRelCodeArr = _g.TKVStr(jRequestConfig, "first").Split('|');
                    string[] countRelCodeArr = _g.TKVStr(jRequestConfig, "count").Split('|');
                    string[] maxRelCodeArr = _g.TKVStr(jRequestConfig, "max").Split('|');

                    firstRelCode = firstRelCodeArr[0];
                    countRelCode = countRelCodeArr[0];
                    maxRelCode = maxRelCodeArr[0];

                    if (maxRelCodeArr.Length > 1) maxRelField = maxRelCodeArr[1];


                    if (firstRelCode.Length != 0)
                    {
                        // relation is taken from the childTable which is also the source table for data extraction
                        relFirst = childTable.tableRelations[firstRelCode];
                        withFirstAgreggate = relFirst.type == TableRelationTypes.ONE2MANY;
                    }

                    if (countRelCode.Length != 0)
                    {
                        // relation is taken from the childTable which is also the source table for data extraction
                        relCount = childTable.tableRelations[countRelCode];
                        withCountAgreggate = relCount.type == TableRelationTypes.ONE2MANY;

                    }

                    if (maxRelCode.Length != 0)
                    {
                        // relation is taken from the childTable which is also the source table for data extraction
                        relMax = childTable.tableRelations[maxRelCode];
                        withMaxAgreggate = relMax.type == TableRelationTypes.ONE2MANY;

                    }
                }
                else
                {
                    // simply log message!
                    DALData.DAL.LogGlobalMessage(jRequestConfig);

                }

            }



            // define what goes to cols collection and what goes to the return ('ret') sql string...
            if (mode == SQLModes.INSERT)
            {
                // => #INSERT#
                if (includeColumns != null) cols = includeColumns;
                ret = "insert into [" + tableName + "] (" + SQLText(fromInsert: true, includeColumns: cols) + ") select ";
            }
            else if (mode == SQLModes.UPDATE)
            {
                ret = "update " + tableName + " set ";
                cols = (includeColumns != null ? includeColumns : cols.Where(c => c.keyPosition == -1).ToList());
            }
            else if (mode == SQLModes.SORT)
            {
                if (sortColumns == null)
                    sortColumns = cols.Where(c => c.sortPosition != -1).OrderBy(x => x.sortPosition).ToList();
                cols = sortColumns;
                if (cols.Count() != 0) ret = " order by ";
            }
            else if (condOrDel)
            {
                if (fromLink)
                {

                    cols = null;    // to prevent processing of filter columns based on default key/keyField(s) entries
                    string whereExpr = "";
                    for (int vi = 0; vi < whereColumns.Count; vi++)
                    {
                        // build entire where condition for link object
                        int valCount = whereValCounts.ElementAt(vi);
                        string expOrVals = "";

                        bool isParent = true;
                        string whereField;

                        if (whereColumns != null)
                        {
                            ColumnInfo whereColunm = whereColumns[vi];
                            isParent = whereColunm.table.tableCode != tableCode;
                            whereField = whereColunm.name;
                        }
                        else
                        {
                            whereField = rel.linkTableFieldA;
                        }

                        // resolve where column object
                        ColumnInfo col = isParent ? parentTable.columns.Find(c => c.name == whereField) : columns.Find(c => c.name == whereField); //? for investigation with intermediate link table

                        string[] valArr = null;
                        if (whereKeyVals != null) valArr = whereKeyVals.ElementAt(0).Split(_g.PARAMS_VAL_DELIM_CHAR);

                        for (int valIdx = 0; valIdx < valCount; valIdx++)
                        {
                            string valCondOperator = "=";

                            if (valArr != null && col.type == "String")
                            {
                                string condValue = valArr[valIdx];
                                if (condValue.EndsWith("%") || condValue.IndexOf("_") != -1) valCondOperator = SQL_LIKE;
                            }

                            expOrVals += _g.GetDelim(valIdx == 0, " Or ") + String.Format("{0}.{1} {2} " + PARAM_PREFIX + "p{3}", isParent ? "L" : "T", col.name, valCondOperator, ctr);
                            ctr++;
                        }

                        // add ANDed condition to the main where expression
                        whereExpr += (whereExpr.Length != 0 ? " And " : "") + "(" + expOrVals + ")";

                    }

                    ret += " where (" + whereExpr + ")";

                }
                else
                {
                    if (byGroup)    // if filter by group
                        whereColumns = cols.Where(c => c.groupPosition != -1).OrderBy(x => x.groupPosition).ToList();
                    else if (whereColumns == null)   // if whereColumns is not specified, use defined key field(s)
                        whereColumns = cols.Where(c => c.keyPosition != -1).OrderBy(x => x.keyPosition).ToList();

                    cols = whereColumns;
                    // results to...
                    // 1. "delete from tbl"
                    // 2. "delete from tbl where "
                    // 3. " where "
                    // 4. ""
                    ret = (mode == SQLModes.DELETE ? "delete from " + tableName : "") + (cols.Count() != 0 ? " where " : "");
                }

            }
            else if (mode == SQLModes.SELECT)
            {
                ret = "select ";


                // if cols becomes null because includeColumns is null, all fields will be selected
                cols = includeColumns;
            }
            else if (fromInsert && includeColumns != null)
            {
                // if expression to be constructed is coming from the insert command. see => #INSERT#
                cols = includeColumns;
            }

            // process cols collection when available...
            if (cols != null)
            {
                int colIdx = 0; // absolute column index during iteration  in the foreach
                if (condOrDel && whereValCounts == null)
                {
                    // initialize whereValCounts to 1 for each where field
                    whereValCounts = new List<int>();
                    foreach (ColumnInfo col in cols) whereValCounts.Add(1);
                }

                foreach (ColumnInfo col in cols)
                {
                    isInitial = (ctr == initCtr);

                    // **** Check column exclusions ****
                    if (((mode == SQLModes.INSERT || fromInsert) && (col.isLocked || col.isLockedBy || col.isUpdated || col.isUpdatedBy)) ||
                         ((mode == SQLModes.UPDATE) && (col.isLocked || col.isLockedBy || col.isCreated || col.isCreatedBy))) continue;

                    // **** Process columns ****
                    if (mode == SQLModes.FIELDS || mode == SQLModes.SELECT)
                    {
                        // specified set of fields are selected
                        // plain comma delmited field list
                        // string fmt = "[{0}]";
                        // results to ...
                        // 1. [field_0], [field_1], [field_2], ..., [field_#]
                        ret += _g.GetDelim(isInitial) + String.Format(fmt, col.name);
                        ctr++;
                    }
                    else if (mode == SQLModes.SORT)
                    {
                        //sortDirections
                        bool isDescending = sortDirections[colIdx];

                        ret += _g.GetDelim(isInitial) + String.Format(fmt, col.name);
                        if (sortDirections != null) if (!sortDirections[colIdx]) ret += " desc";

                        ctr++;
                    }
                    else if (mode == SQLModes.INSERT)
                    {
                        // comma delimited select field
                        // results to ....
                        // 1. @p0 as [field_0], @p1 as [field_1], ... ,@p# as [field_#]
                        ret += _g.GetDelim(isInitial) + String.Format("{0}p{1}{2}{3}", PARAM_PREFIX, ctr, FIELD_ALIAS_LINK, col.name);
                        ctr++;
                    }
                    else if (condOrDel)
                    {

                        // And operator delimited field value assignment/evaluation
                        // results to ...
                        // 1. [field_0] = @p0 And [field_1] = @p1 ... And ... [field_#] = @p#

                        // if available set of key values is less than the number of keyField columns
                        // ignore the rest. eg. key => "8289,8280,8269" keyField="RF_CLASS`RF_TYPE"
                        // since only RF_CLASS has the equivalent key fields, creation of where expression
                        // will stop after processing all where expressions for RF_CLASS.

                        if (whereValCounts.Count > colIdx)
                        {

                            int valCount = whereValCounts.ElementAt(colIdx);

                            string[] valArr = null;
                            if (whereKeyVals != null) valArr = whereKeyVals.ElementAt(colIdx).Split(_g.PARAMS_VAL_DELIM_CHAR);

                            string expOrVals = "";

                            for (int valIdx = 0; valIdx < valCount; valIdx++)
                            {

                                string valCondOperator = "=";
                                if (valArr != null && col.type == "String")
                                {
                                    string condValue = valArr[valIdx];
                                    if (condValue.EndsWith("%") || condValue.IndexOf("_") != -1) valCondOperator = SQL_LIKE;
                                }

                                expOrVals += _g.GetDelim(valIdx == 0, " Or ") + String.Format("{0} {1} " + PARAM_PREFIX + "p{2}", col.name, valCondOperator, ctr);
                                ctr++;
                            }

                            ret += _g.GetDelim(isInitial, " And ") + "(" + expOrVals + ")";
                        }


                    }
                    else if (mode == SQLModes.UPDATE)
                    {
                        // comma delimited field value assignment/evaluation
                        // results to ...
                        // 1. (field enumeration) => [field_0] = @p0, [field_1] = @p1 ..., ... [field_#] = @p#

                        ret += _g.GetDelim(isInitial, ", ") + String.Format("{0} = " + PARAM_PREFIX + "p{1}", col.name, ctr);

                        ctr++;

                    }
                    colIdx++;
                } // end of foreach cols


            }   // end of cols != null
            else
            {
                // if select and fromLinked table, include parent id
                if (mode == SQLModes.SELECT)
                    ret += "T.*";   // extract all fields from the main/childTable

            }   // end of if cols is not null

            // after iteration of included columns, check if additional column processing is necessary
            if (mode == SQLModes.SELECT && rel != null)
                if (fromLink && rel.type == TableRelationTypes.LINK)
                    ret += ", L." + rel.linkTableFieldA + FIELD_ALIAS_LINK + _g.FIELD_PARENT_LINK_ALIAS;
            if (withFirstAgreggate)
                ret += ", " + relFirst.selectAgregate("first") + FIELD_ALIAS_LINK + _g.FIELD_CHILD_FIRST_ALIAS;
            if (withCountAgreggate)
                ret += ", " + relCount.selectAgregate("count") + FIELD_ALIAS_LINK + _g.FIELD_CHILD_COUNT_ALIAS;
            if (withMaxAgreggate)
                ret += ", " + relMax.selectAgregate("max", maxRelField) + FIELD_ALIAS_LINK + _g.FIELD_CHILD_MAX_ALIAS;

            //append table name and apply condition on select statement
            if (mode == SQLModes.SELECT)
            {
                string fromClause;
                string dataLinkField = keyCol.name;

                if (rel != null)
                {
                    if (rel.localField != "") parentField = rel.localField;
                    if (rel.foreignField != "") dataLinkField = rel.foreignField;
                }

                if (parentTable != null && parentField != null)
                {
                    /* 
                     * SELECT T.* 
                     * FROM tblLink AS P INNER JOIN tblChild AS T ON P.lnk_chi_id = T.chi_id
                        WHERE (((P.lnk_par_id)=5));
                     */
                    fromClause = String.Format("{0} as L inner join {1} as T on [L].{2} = T.{3}",
                        parentTable.tableName, tableName, parentField, dataLinkField);
                }
                else if (fromLink)
                {
                    fromClause = rel.linkFromClause;
                }
                else
                {
                    fromClause = tableName + " as T ";
                }

                ret += " from " + fromClause +
                    (noCond ? "" : SQLText(SQLModes.CONDITION, ctr, byGroup: byGroup,
                        whereColumns: whereColumns, whereValCounts: whereValCounts,
                        parentTable: parentTable, childTable: childTable, keyVals: keyVals)) +
                    (noSort ? "" : SQLText(SQLModes.SORT, sortDirections: sortDirections, sortColumns: sortColumns));
            }
            //apply condition on update statement
            if (mode == SQLModes.UPDATE && !noCond) ret += SQLText(SQLModes.CONDITION, ctr, whereColumns: whereColumns);

            //append SQL terminator ;
            if (mode != SQLModes.CONDITION && mode != SQLModes.SORT && !fromInsert) ret += ";"; // append terminating semicolon...

            return ret;
        }

        public List<ColumnInfo> ColumnsFromIndices(string colIndices)
        {
            if (colIndices.Length == 0) return null;
            List<ColumnInfo> ret = new List<ColumnInfo>();
            string[] varArr = colIndices.Split(_g.PARAMS_DELIM_CHAR);
            foreach (string col in varArr)
            {
                ColumnInfo foundColumn = null;
                if (col.All(char.IsDigit))
                {
                    // column index was specified
                    foundColumn = columns.ElementAt(Convert.ToInt32(col));


                }
                else
                {
                    // column name was specified
                    foundColumn = columns.Find(c => c.name == col);
                }

                if (foundColumn != null) ret.Add(foundColumn);

            }
            return ret;
        }

        public Dictionary<string, dynamic> BuildUpdateParams(JObject values, List<ColumnInfo> colWhere = null)
        {
            Dictionary<string, dynamic> ret = new Dictionary<string, dynamic>();

            List<ColumnInfo> colFields = new List<ColumnInfo>();
            //if (colWhere == null) colWhere = new List<ColumnInfo>();
            //List<ColumnInfo> colWhere = new List<ColumnInfo>();

            // this method must also be capable of profiling which is being
            // able to remember available parameter from the values passed
            // on the initial call.

            // process regular fields

            // search for key parameters

            // search for updated by parameter (i.e. __uid__ parameter)

            string keyColName = this.keyCol.name;

            foreach (JProperty jp in (JToken)values)
            {
                //ret.Add(jp.Name, jp.Value);
                if (colWhere == null && jp.Name == keyColName)
                {
                    if (colWhere == null)
                    {
                        colWhere = new List<ColumnInfo>();
                        colWhere.Add(this.keyCol);
                    }
                    continue;
                }

                //ColumnInfo col=this.columns.Find(ColumnInfo c => c.name)

            }

            return ret;
        }

        public Dictionary<string, dynamic> BuildCommandParamsFromKeys(string keyVals, string keyIndices = "", DALTable parentTable = null)
        {
            if (keyVals.Length == 0) return null;

            Dictionary<string, dynamic> retVal = new Dictionary<string, dynamic>();
            List<ColumnInfo> cols = new List<ColumnInfo>();


            string[] whereFields = keyIndices.Split(_g.PARAMS_DELIM_CHAR);
            for (int wi = 0; wi < whereFields.Length; wi++)
            {
                ColumnInfo col = null;
                string colName = whereFields[wi];//.Split('|')[0];
                if (parentTable != null)
                {
                    // if parent table is not null, search this first
                    col = parentTable.columns.Find(c => c.name == colName);
                }
                if (col == null) col = columns.Find(c => c.name == colName);
                if (col != null) cols.Add(col);
            }

            // The following codes were replaced on 2020-05-24 to accommodate multi-table parameter values
            //if (parentTable != null)
            //{
            //    // build parameters based on DALRelation link type setting
            //    // a single ColumnInfo which is the key field of the parentTable

            //    cols = (keyIndices.Length == 0 ? new List<ColumnInfo>() { parentTable.keyCol } : parentTable.ColumnsFromIndices(keyIndices));
            //}
            //else
            //{
            //    cols = (keyIndices.Length == 0 ? keyCols : ColumnsFromIndices(keyIndices));
            //}

            string[] varArr = keyVals.Split(_g.PARAMS_DELIM_CHAR);  // get array of keyValue groups
            int colIdx = 0;
            int valIdx = 0;

            foreach (ColumnInfo col in cols)    // iterates through a single-element array when dealing with a linkTable request
            {
                // if available set of key values is less than the number of keyField columns
                // ignore the rest. eg. key => "8289,8280,8269" keyField="RF_CLASS`RF_TYPE"
                // since only RF_CLASS has the equivalent key fields, creation of parameters
                // will stop after processing all parameters for RF_CLASS.

                if (colIdx >= varArr.Length) break;

                string[] pVals = varArr[colIdx].Split(_g.PARAMS_VAL_DELIM_CHAR);    // get individual value in a keyValue group
                foreach (string pVal in pVals)
                {
                    //vals.Add(col.name, DALData.DAL.DALCastValue(col.type, pVal));
                    string[] pValArr = pVal.Split('|'); // this separates the optional operator from the actual value
                    retVal.Add(PARAM_PREFIX + "p" + valIdx, DALData.DAL.DALCastValue(col.type, pValArr[0]));
                    valIdx++;
                }
                colIdx++;
            }


            //Dictionary<string, dynamic> retVal = BuildCommandParams(vals, cols);

            return retVal;
        }

        public Dictionary<string, dynamic> BuildCommandParams(JObject args, List<ColumnInfo> cols)
        {
            Dictionary<string, dynamic> ret = new Dictionary<string, dynamic>();
            int idx = 0;
            foreach (ColumnInfo col in cols)
            {
                ret.Add(PARAM_PREFIX + "p" + idx, DALData.DAL.DALCastValue(col.type, _g.TKVDyn(args, col.name, col.type)));
                idx++;
            }
            return ret;
        }


    }

    public class DALTableFieldParams
    {
        public DALTableFieldParams(JObject data,
            DALStamps stamps,
            DALTable table,
            bool isUpdating = true,
            List<ColumnInfo> keyFields = null)
        {

            this.SQLText = "";
            this.columns = null;
            this.parameters = null;
            this.tempNewId = 0;

            this.stamps = stamps;
            this.isUpdating = isUpdating;
            this.table = table;

            this.keyFields = keyFields != null ? keyFields : table.keyCols;
            if (this.keyFields == null) this.keyFields = new List<ColumnInfo>();

            FormatData(data, this.stamps);

        }

        private DALStamps stamps { set; get; }
        private bool isUpdating { set; get; }

        private DALTable table { set; get; }
        public List<ColumnInfo> keyFields { set; get; }

        public string SQLText { set; get; }
        public List<ColumnInfo> columns { set; get; }
        public Dictionary<string, dynamic> parameters { set; get; }
        public Int64 tempNewId { set; get; }

        public void FormatData(JObject data, DALStamps stamps, bool reset = false)
        {
            Dictionary<string, dynamic> ret = new Dictionary<string, dynamic>();
            Dictionary<string, dynamic> key = new Dictionary<string, dynamic>();

            ColumnInfo tmpCol;

            // add date/time stamp to raw data
            if (isUpdating && stamps.updatedField != null)
            {
                ret.Add(stamps.updatedField.name, stamps.stampDateTime);
                if (stamps.updatedByField != null) ret.Add(stamps.updatedByField.name, stamps.userId);
            }
            else if (stamps.createdField != null)
            {
                ret.Add(stamps.createdField.name, stamps.stampDateTime);
                if (stamps.createdByField != null) ret.Add(stamps.createdByField.name, stamps.userId);
            }


            //string user = _g.TKVStr(args,_g)

            if (isUpdating)
            {
                // when updating, place key value(s) at the end of the parameters list
                foreach (JProperty jp in (JToken)data)
                {
                    // check if property is one of the fields defined in the table,
                    // process the next property if non-existent
                    if (!table.colExist(jp.Name)) continue;

                    if (keyFields.Find(c => c.name == jp.Name) == null)
                    {
                        // add to regular field collection
                        ret.Add(jp.Name, DALData.DAL.DbNullIfNull(jp));
                    }
                    else
                    {
                        // add to key value collection
                        key.Add(jp.Name, jp.Value);
                    }
                }

                // push all key column parameters at the end of the list
                foreach (KeyValuePair<string, dynamic> kp in key)
                {
                    ret.Add(kp.Key, kp.Value);
                }

            }   // end of updating
            else
            {
                // when creating a record, where condition is not applicable, therefore
                // separation of regular and key field(s) is no longer necessary
                foreach (JProperty jp in (JToken)data)
                {
                    // check if property is one of the fields defined in the table,
                    // process the next property if non-existent
                    if (!table.colExist(jp.Name)) continue;

                    ret.Add(jp.Name, DALData.DAL.DbNullIfNull(jp));
                }
                if (table.autoKey)
                {
                    // if autoKey property is true, auto-generate
                    // the numeric field to be assigned to the key field
                    // of the table

                    string keyName = table.keyCols.ElementAt(0).name;

                    // when crreating a new record, if key field is existing in the data,
                    // assume the value to be the correct new id
                    // else create a new one using the table.NewAutoId get property

                    if (!ret.ContainsKey(keyName))
                    {
                        Int64 newAutoId = table.NewAutoId;
                        ret.Add(keyName, newAutoId);
                    }

                }

            }   // end of test if operation is updating or creating of record

            if (columns == null || reset)
            {
                // process collection of columns for SQL Text generation
                if (columns != null) columns.Clear();
                columns = new List<ColumnInfo>();
                if (isUpdating)
                {
                    // when updating record, do not include key fields in the selection
                    foreach (string fieldName in ret.Keys)
                    {
                        if (keyFields.Find(c => c.name == fieldName) == null)
                        {
                            // add non-keyfield columns only
                            tmpCol = table.columns.Find(c => c.name == fieldName);
                            if (tmpCol != null) columns.Add(tmpCol);

                        }
                        else
                        {
                            // if field to include is one of the keyfields,
                            // exit for loop because the remaining fields if any
                            // belong to the key fields and not to be included in
                            // updates.
                            break;
                        }
                    }
                }
                else
                {
                    // when creating record
                    foreach (string fieldName in ret.Keys)
                    {
                        tmpCol = table.columns.Find(c => c.name == fieldName);
                        columns.Add(tmpCol);
                    }
                }
            }
            if (this.SQLText == "" || reset)
                this.SQLText = this.table.SQLText(
                    isUpdating ? SQLModes.UPDATE : SQLModes.INSERT,
                    includeColumns: columns, whereColumns: keyFields);
            //if (this.SQLText == "" || reset)
            //    this.SQLText = this.table.SQLText(
            //        isUpdating ? "update" : "insert");

            // when creating a record, record the temporary id of a new record
            if (data.ContainsKey(_g.KEY_NEW_TEMP_ID))
                this.tempNewId = _g.TKV64(data, _g.KEY_NEW_TEMP_ID);

            // set object parameters to the collection of from input and auto-generated values
            this.parameters = ret;


        }   // end of FormatData

    }

    public class DALTableUpdateParams
    {
        public DALTableUpdateParams(DALTable table, JObject data, JObject args = null)
        {
            this.keyField = null;
            this.stampField = null;
            this.fields = new List<ColumnInfo>();
            this.fieldsToUpdate = null;

            this.trackCommandParams = null;
            this.linkCommandParams = null;

            this.table = table;
            this.data = data;
            this.args = args;

            this.keyName = table.keyCol.name;
            this.keyValue = _g.TKV64(data, this.keyName);

            // if keyvalue is a negative number, then INSERT
            this.isNew = (keyValue < 0);

            // if keyvalue is a positive number and the only data passed is the keyValue, then DELETE
            this.isDelete = (keyValue > 0 && data.Count == 1);

            // get user login name
            this.uid = "system";

            // initialize keyValue parameters
            this.paramKeyValue = -1;
            this.paramKeyValuePosition = -1;

            DALData.DAL.globalError = "";

            // get passed user id
            if (args != null)
                if (args.ContainsKey(_g.KEY_USER_ID)) uid = _g.TKVStr(args, _g.KEY_USER_ID, "system");

            if (ProcessData())
            {
                this.result = _g.RES_SUCCESS;
                this.errorMessage = DALData.DAL.globalError;
            }
            else
            {
                this.result = _g.RES_ERROR;
                this.errorMessage = "";
            }

        }

        private bool ProcessData()
        {
            // creates command texts and parameters based on the request type (i.e. UPDATE, INSERT or DELETE)
            try
            {

                if (isDelete)
                    CreateDeleteCommandParams();
                else
                {
                    if (isNew)
                    {
                        this.updateParams = new Dictionary<string, dynamic>();
                        this.output = new JObject();
                    }
                    foreach (JProperty jp in (JToken)data)
                    {
                        string fieldName = jp.Name;

                        if (fieldName == keyName && this.keyField == null)
                        {
                            this.keyField = table.keyCol;

                            // if new record include key field in the fields to be passed to Insert SQLText 
                            if (isNew)
                            {
                                var newKey = data[String.Format("__{0}__", this.keyName)];
                                if (newKey == null)
                                {
                                    // new key is NOT supplied by the client
                                    this.newKey = table.NewAutoId;

                                }else
                                {
                                    // new key is SUPPLIED by the client
                                    this.newKey = Convert.ToInt64(newKey);

                                }

                                if (this.newKey == -1)
                                {
                                    // error has occured and message is set to globalError string
                                    throw new Exception("Cannot get new record ID. " + DALData.DAL.globalError);
                                }

                                // add new key value and temporary key value to command output fields
                                this.output.Add(this.keyField.name, this.newKey);

                                // add reference to original value passed from the client in order to locate the
                                // new client record to update (e.g. keyValue, reference code, etc.)
                                this.output.Add(this.keyField.name + "_REF", keyValue);

                                this.fields.Add(this.keyField);

                                // add command parameter to EDITED record parameters collection
                                this.paramKeyValue = this.newKey;
                                this.paramKeyValuePosition = this.updateParams.Count;

                                this.updateParams = _g.MergeParams(this.updateParams, this.paramKeyValue);

                                // map old key value to the new key value. this will be returned to the client
                                // in order to update -keyValues on the client side with the new set of key values
                            }

                            continue;
                        }

                        // if fieldname is key to collection of linked tables
                        if (fieldName == _g.KEY_LINK_CODES)
                        {
                            if (!isDelete) this.CreateLinkCommandParams();
                            continue;
                        }

                        if (this.stampField == null)
                        {

                        }

                        // add column to initial updatable fields collection
                        ColumnInfo col = table.GetColumnByName(fieldName);
                        if (col != null)
                        {
                            // add updatable fields without checking if value change has been made
                            this.fields.Add(col);


                            if (isNew)
                            {
                                string strVal = jp.Value.ToString();
                                if (strVal.StartsWith("{") && strVal.EndsWith("}"))
                                {
                                    // value must be replaced with evaluated expression
                                    // parse parameters inside the curly brackets (i.e. format|year)
                                    string[] prms = strVal.TrimStart('{').TrimEnd('}').Split('|');

                                    // get expression format string
                                    string format = prms[0];
                                    // get year value
                                    int year = prms.Length >= 2 ? Convert.ToInt32(prms[1]) : -1;
                                    // get cached value-reset flag
                                    bool reset = (prms.Length >= 3 ? (prms[2] == "1") : false);

                                    // evaluate expression and assign the result as new
                                    // value of the JParameter object
                                    jp.Value = table.NewRefNo(col.name, format, year, reset);

                                    // add server-side evaluated field value to command output
                                    this.output.Add(col.name, jp.Value);

                                }

                                // add command parameter to NEW record parameters collection
                                this.updateParams = _g.MergeParams(this.updateParams, col, jp);
                            }
                        }
                    }

                }


                if (isNew)
                {
                    CreateInsertCommandParams();
                }
                else
                {
                    // udate record
                    CreateTrackCommandParams();
                    CreateUpdateCommandParams();
                }

                return true;


            }
            catch (Exception e)
            {
                DALData.DAL.globalError = "DALTableUpdateParams/ProcessData(): " + e.Message;
                return false;
            }

        }

        private List<ColumnInfo> fields { get; set; }
        public List<ColumnInfo> fieldsToUpdate { get; set; }

        public ColumnInfo keyField { get; set; }
        public ColumnInfo stampField { get; set; }
        public Dictionary<string, dynamic> updateParams { get; set; }

        public List<CommandParam> trackCommandParams { get; set; }
        public List<CommandParam> linkCommandParams { get; set; }

        public List<CommandParam> deleteCommandParams { get; set; }

        public CommandParam newRecordCommandParam { get; set; }

        public Int64 paramKeyValue { get; set; }
        public Int64 paramKeyValuePosition { get; set; }

        private string uid { get; set; }

        private JObject args { get; set; }
        private JObject data { get; set; }
        private DALTable table { get; set; }

        private string keyName { get; set; }
        private Int64 keyValue { get; set; }
        private Int64 newKey { get; set; }

        private JObject output { get; set; }

        private bool isNew { get; set; }
        private bool isDelete { get; set; }

        public string result { get; set; }
        public string errorMessage { get; set; }

        public Dictionary<Int64, Int64> newKeys { get; set; }

        private void CreateLinkCommandParams()
        {
            // ************************ expected data format **************************
            /* row.__links__ = [
             *      {
             *          "table_code": <code>,
             *          "child_ids" : <[id1,id2,...,id#]>,
             *          "action"    : <action>                  // from enum TableRelationActions current:ADD:'add', REMOVE:'remove
             *      }
             * ]
             * row.<keyCol.name>
             * 
             * post data eg. **************************************************************
             * {
             * 	"an":[ 
             * 			{
             * 				"AN_TITLE": "Test for Linked Records1",
             * 				"AN_ID": 10362, 
             * 				"__links__": [
             * 					{	"table_code":"ft",
             * 						"child_ids": "50,3,65",
             * 						"action":"add"
             * 					},
             * 					{	"table_code":"an",
             * 						"child_ids": "10279,10220,10200",
             * 						"action":"remove"
             * 					}
             * 				]
             * 			 }
             * 		],
             * 	"__config__": {"useCommonNewKey": false}
             * }
            */
            // populate linkCommandParams collection property
            JArray linkData = (JArray)data[_g.KEY_LINK_CODES];

            // Get current record keyValue to be used as parentId argument
            Int64 parentId = _g.TKV64(data, table.keyCol.name);

            // loop through link tokens
            foreach (JObject j in linkData)
            {
                //DALRelation rel =new DALRelation(_g.TKVStr(j, "title_code"))
                //table.tableRelations[_g.TKVStr(j, "table_code")].CreateNewLinkCommandParams(_g.TKV64(data, table.keyCol.name), _g.TKVStr(j, "child_ids"))

                string cTableCode = _g.TKVStr(j, "table_code");
                string[] cTableCodeArr = cTableCode.Split('-');
                string childTableCode = cTableCodeArr[0];
                string childType = cTableCodeArr.Length == 1 ? "" : cTableCodeArr[1];

                string childIds = _g.TKVStr(j, "child_ids");
                string action = _g.TKVStr(j, "action");

                // if link is made at the same time the record is created
                bool simultaneous = _g.TKVBln(j, "simultaneous");   

                DALRelation rel;

                if (table.tableRelations.ContainsKey(childTableCode))
                {
                    rel = table.tableRelations[childTableCode];
                }
                else
                {
                    DALTable childTable =  AppDataset.AppTables[childTableCode];
                    rel = new DALRelation("lnk", table, childTable,childType: childType);
                }

                if (this.linkCommandParams == null) this.linkCommandParams = new List<CommandParam>();

                if (action == TableRelationActions.ADD)
                {

                    this.linkCommandParams = _g.MergeCommandParams(this.linkCommandParams,
                        rel.CreateNewLinkCommandParams(parentId, childIds, false, simultaneous));

                    // Create table track add link(s) record
                    this.trackCommandParams = _g.MergeCommandParams(this.trackCommandParams,
                        table.CreateChangeTrackCommand(uid, TrackingActions.CREATE,
                                    rel.linkTableFieldB, parentId.ToString(),
                                    "Added Link(s): " + childIds, tableCode: rel.linkTableName));

                }
                else if (action == TableRelationActions.REMOVE)
                {
                    CommandParam cmd = rel.CreateDeleteLinkCommandParam(parentId, childIds);
                    this.linkCommandParams = _g.MergeCommandParams(this.linkCommandParams,
                            cmd);

                    // Create table track remove link(s) record
                    this.trackCommandParams = _g.MergeCommandParams(this.trackCommandParams,
                                    table.CreateChangeTrackCommand(uid, TrackingActions.DELETE,
                                        rel.linkTableFieldB, parentId.ToString(),
                                        "Removed Link(s): " + childIds, tableCode: rel.linkTableName)
                                );
                }

            }
        }

        private void CreateTrackCommandParams()
        {
            if (fields.Count != 0)
            {
                // Tracking changes
                // select record to be updated
                // initialize record dictionary
                // Dictionary<string, dynamic> rec = new Dictionary<string, dynamic>();
                // create select statement
                string sql = table.SQLTextSelectForTracking(this.fields);
                Dictionary<string, dynamic> prms = new Dictionary<string, dynamic>();

                // where clause parameter which contains the key field value
                prms.Add(table.PARAM_PREFIX + "p" + fields.Count, data[keyField.name]);

                // retreive current record to filter only the fields that are to change
                List<Dictionary<string, dynamic>> rec = DALData.DAL.GetDictionaryArray(new CommandParam(sql, prms));
                if (rec.Count != 0)
                {
                    Dictionary<string, dynamic> row = rec[0];

                    foreach (KeyValuePair<string, dynamic> val in row)
                    {
                        ColumnInfo col = table.GetColumnByName(val.Key);

                        // compare values
                        //if((dynamic)val.Value != (dynamic)data[val.Key])
                        if (DALData.DAL.DALCastValue(col.type, val.Value) !=
                            DALData.DAL.DALCastValue(col.type, data[val.Key]))
                        {
                            // add column to fieldsToUpdate
                            if (this.fieldsToUpdate == null) this.fieldsToUpdate = new List<ColumnInfo>();
                            this.fieldsToUpdate.Add(col);

                            // value has changed, log an entry into the tracking table
                            if (table.tableChangeTrack != null)
                            {
                                // if update tracking table is defined
                                this.trackCommandParams = _g.MergeCommandParams(this.trackCommandParams,
                                    table.CreateChangeTrackCommand(
                                                        uid, TrackingActions.UPDATE, col.name,
                                                        _g.TKVStr(data, keyField.name),
                                                        DALData.DAL.DALCastValue("string", val.Value, "")
                                                    )
                                                );
                            }
                        }
                        else
                        {
                            // nothing to track
                        }

                    }
                }
                else
                {
                    // record does not exist, nothing to update
                }

            }
        }

        private void CreateDeleteCommandParams()
        {
            if (this.deleteCommandParams == null) this.deleteCommandParams = new List<CommandParam>();

            // check table type. if lookup item, prevent deletion when still being used in other tables


            // if table with linked type table relation object, delete child objects first
            // before deleting the parent record
            foreach (string relKey in table.tableRelations.Keys)
            {
                DALRelation rel = table.tableRelations[relKey];
                if (rel.type == TableRelationTypes.LINK)
                {
                    // check if there are records from link table
                    this.deleteCommandParams.Add(rel.CreateDeleteLinkCommandParam(this.keyValue));
                }
            }

            // delete parent item
            this.deleteCommandParams.Add(new CommandParam(table.SQLText(SQLModes.DELETE),
                    new Dictionary<string, dynamic>() { { table.PARAM_PREFIX + "p0", this.keyValue } }, _table: table));
        }

        private void CreateInsertCommandParams()
        {
            ColumnInfo colCreated = table.GetColumnByBoolParam("isCreated");

            if (colCreated != null)
            {
                // if created date/time stamp field is available, add to the collection of
                // columns to be updated
                this.fields.Add(colCreated);

                // add date/time stamp value to the parameters
                // this.updateParams=_g.MergeParams(this.updateParams, _g.DateTimeNow);
                _g.MergeParams(_g.DateTimeNow, this.updateParams);
            }

            ColumnInfo colCreatedBy = table.GetColumnByBoolParam("isCreatedBy");
            if (colCreatedBy != null)
            {
                // if created by stamp field is available, add to the collection of
                // columns to be updated
                this.fields.Add(colCreatedBy);
                // add user login value to the parameters
                // this.updateParams = _g.MergeParams(this.updateParams, uid);
                _g.MergeParams(uid, this.updateParams);
            }

            this.newRecordCommandParam =
                new CommandParam(table.SQLText(SQLModes.INSERT, includeColumns: this.fields),
                    this.updateParams, _tempKey: keyValue, _newKey: newKey, _table: table, _cmdOutput: output, _paramKeyValuePosition: this.paramKeyValuePosition, _paramKeyValue: this.paramKeyValue);

        }

        private void CreateUpdateCommandParams()
        {
            if (this.fieldsToUpdate != null)
            {
                // if there are fields to update

                // generate parameters dictionary
                this.updateParams = new Dictionary<string, dynamic>();
                int i;
                for (i = 0; i < fieldsToUpdate.Count; i++)
                {
                    dynamic paramValue = data[fieldsToUpdate[i].name];
                    if (paramValue == null) paramValue = DBNull.Value;
                    this.updateParams.Add(table.PARAM_PREFIX + "p" + i, paramValue);
                }

                // check stamp fields
                ColumnInfo colUpdated = table.GetColumnByBoolParam("isUpdated");
                ColumnInfo colUpdatedBy = table.GetColumnByBoolParam("isUpdatedBy");
                DateTime dtUpdated = _g.DateTimeNow;

                if (colUpdated != null)
                {
                    // if update date/time stamp field is available, add to the collection of
                    // columns to be updated
                    this.fieldsToUpdate.Add(colUpdated);

                    // add date/time stamp value to the parameters
                    this.updateParams.Add(table.PARAM_PREFIX + "p" + this.updateParams.Count, _g.DateTimeNow);
                }

                if (colUpdatedBy != null)
                {
                    // if update date/time stamp field is available, add to the collection of
                    // columns to be updated
                    this.fieldsToUpdate.Add(colUpdatedBy);

                    // add user login value to the parameters
                    this.updateParams.Add(table.PARAM_PREFIX + "p" + this.updateParams.Count, uid);
                }

                // add single-key field filter to the parameters
                this.paramKeyValuePosition = this.updateParams.Count;
                this.paramKeyValue = (Int64)data[keyField.name];

                this.updateParams.Add(table.PARAM_PREFIX + "p" + this.paramKeyValuePosition, this.paramKeyValue);

            }
            else
            {
                // nothing to update. all submitted values are already the current value of the 
                // fields in the selected record.
            }

        }

    }

    public class DALTableLink
    {
        public DALTableLink(string code, Dictionary<string, DALTable> tables, JObject args = null)
        {
            //"user->uprm|uprm_user_id"
            string uid = args == null ? null : _g.TKVStr(args, _g.KEY_USER_ID);
            string[] codeArr = _g.Split(code, "->");

            //lnk_chi_id->chi|chi_id
            //lnk|lnk_chi_id->chi|chi_id

            // Master table code and key field name
            this.code = codeArr[0];
            this.table = tables[this.code];
            this.key = table.keyCols[0].name;

            this.stamps = new DALStamps(table.columns, uid);

            if (codeArr.Length > 1)
            {
                hasChild = true;
                string[] childArr = codeArr[1].Split('|');
                this.childCode = childArr[0];
                this.childParentKey = childArr[1];

                this.childTable = tables[this.childCode];
                this.childKey = this.childTable.keyCols[0].name;

                this.childStamps = new DALStamps(childTable.columns, uid);
            }
            else
            {
                hasChild = false;
                this.childCode = "";
                this.childParentKey = "";
            }
        }
        public string code { get; set; }
        public string key { get; set; }

        public DALStamps stamps { get; set; }
        public DALStamps childStamps { get; set; }

        public DALTable table { get; set; }

        // Child table code
        public string childCode { get; set; }
        // child table 
        public string childParentKey { get; set; }
        public DALTable childTable { get; set; }
        public string childKey { get; set; }

        public bool hasChild { get; set; }

    }

}