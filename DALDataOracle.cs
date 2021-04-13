using System;
using System.Data;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace DataAccess
{
    public class DALDataOracle : DALDataBase
    {

        public DALDataOracle() { DAL_TYPE = "ORACLE"; }
        public override List<ReturnObject> Excute(List<CommandParam> commandParams,bool commit = false)
        {
            return null;
        }
        public override ReturnObject Excute(CommandParam cmdParam, dynamic cmdConnectDynamic = null, dynamic cmdTransactDynamic = null)
        {
            return null;
        }

        public override Int64 GetScalar(string cmdText, Dictionary<string, dynamic> cmdParams = null)
        {
            return -1;
        }
        public override JArray GetJSONArray(CommandParam cmdParam)
        {
            return null;
        }

        public override List<Dictionary<string, dynamic>> GetDictionaryArray(CommandParam cmdParam)
        {
            return null;
        }

        public override dynamic GetDataReaderCommand(CommandParam cmdParam)
        {
            return null;
        }

        public override DataTable GetDataTable(CommandParam cmdParam)
        {
            return null;
        }

        public override ReturnObject GetRecordset(CommandParam cmdParam, bool returnFields = false, bool withFields = false,
            long pageNumber = 0, long pageSize = 0, JArray lookupParams = null)
        {
            return null;
        }

        public override List<Dictionary<string, dynamic>> DALReaderToDictionary(dynamic rdr)
        {
            return null;
        }
        public override JArray DALReaderToJSON(dynamic rdr)
        {
            return null;
        }
        //public override List<List<object>> DALReaderToList(dynamic rdr, long pageNumber = 0, long pageSize = 0, Dictionary<string, Dictionary<string, string>> lkpObj = null)
        public override List<List<object>> DALReaderToList(dynamic rdr, long pageNumber = 0, long pageSize = 0, JArray lkpObj=null)
        {
            return null;
        }

        class DALOracleConnection
        {
            // oledb connection classe
            const string VER = "Oracle";
            public DALOracleConnection()
            {

            }

        }

    }
}
