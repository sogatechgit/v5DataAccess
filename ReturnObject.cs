using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.OleDb;
using Newtonsoft.Json.Linq;
using _g = DataAccess.DALGlobals;


namespace DataAccess
{

    public class ReturnObject

    {
        public ReturnObject()
        {
            result = new ReturnObjectExternal();
        }
        public DataSet primaryDataSet { get; set; }
        public DataTable primaryTable { get; set; }
        public DataRow primaryRow { get; set; }
        public OleDbDataReader primaryReader { get; set; }

        public string returnCode { get; set; }
        public string returnType { get; set; }

        public ReturnObjectExternal result { get; set; }
    }
    public class ReturnObjectExternal
    {
        public ReturnObjectExternal()
        {
            exceptionMessage = "";  // this property will contain a string if unhandled exception error occured
            result = _g.RES_SUCCESS;
            returnString = "";
            debugString = "";
            error = "";
            affectedRecords = -1;
            columns = new List<ColumnInfo>();
            fields = new List<FieldInfo>();
            fieldsNames = new List<string>();
            debugDateTime = DateTime.Now;
            requestDateTime = DateTime.Now;
            debugStrings = new List<string> { };

            this.returnDataParams = new JObject();
            this.requestDateTime = _g.DateTimeNow;
            this.requestDuration = 0;

        }
        public string result { get; set; }
        public string error { get; set; }
        public List<ColumnInfo> columns { get; set; }
        public List<FieldInfo> fields { get; set; }
        public List<string> fieldsNames { get; set; }
        public List<List<object>> returnData { get; set; }
        public Dictionary<string,dynamic> embeddedLookups { get; set; }
        
        public JObject returnDataParams { get; set; }
        public Dictionary<string, List<List<object>>> recordsProps { get; set; }
        public JArray jsonReturnData { get; set; }
        public Int64 recordCount { get; set; }

        public object returnObject { get; set; }
        //public JObject returnJObject { get; set; }
        public string returnString { get; set; }
        public string exceptionMessage { get; set; }
        public string exceptionMarker { get; set; }

        public int affectedRecords { get; set; }

        public string debugSQL { get; set; }
        public List<string> debugStrings { get; set; }
        public string debugString { get; set; }
        public Int64 debugInt64 { get; set; }
        public Int32 debugInt32 { get; set; }
        public Int16 debugInt16 { get; set; }
        public double debugDouble { get; set; }
        public bool debugBoolean { get; set; }
        public DateTime debugDateTime { get; set; }
        public DateTime requestDateTime { get; set; }
        public long requestDuration {get; set;}

    }

}