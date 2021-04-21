﻿#define ORA
//#define OLE
//#define SQL

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAccess
{
    public static class DALData
    {

        public static bool isORA
        {
            get
            {
                return DAL.DAL_TYPE == "ORACLE";
            }
        }

        public static bool isOLE
        {
            get
            {
                return DAL.DAL_TYPE == "JET";
            }
        }

        public static bool isSQL
        {
            get
            {
                return DAL.DAL_TYPE == "MSSQL";
            }
        }
        public static string PARAM_PREFIX
        {
            get
            {
                return isORA ? ":" : "@";
            }
        }

        public static string AS_TBL
        {
            get
            {
                return isORA ? "" : "AS";
            }
        }

        public static string TBL_LDEL
        {
            get
            {
                return isORA ? "\"" : "[";
            }
        }

        public static string TBL_RDEL
        {
            get
            {
                return isORA ? "\"" : "]";
            }
        }

        private static DALDataOleDb _DataOleDb = null;
        public static DALDataOleDb DataOleDb
        {
            get
            {
                if (_DataOleDb == null) _DataOleDb = new DALDataOleDb();
                return _DataOleDb;
            }
        }

        private static DALDataOracle _DataOracle = null;
        public static DALDataOracle DataOracle
        {
            get
            {
                if (_DataOracle == null) _DataOracle = new DALDataOracle();
                return _DataOracle;
            }
        }


        private static DALDataMSSQL _DataMSSQL = null;
        public static DALDataMSSQL DataMSSQL
        {
            get
            {
                if (_DataMSSQL == null) _DataMSSQL = new DALDataMSSQL();
                return _DataMSSQL;
            }
        }

        // Switch Data Provider by assigning the correct Provider class to DAL property

        // To use OleDb, name the OleDbProvider property as DAL and 
        //   rename the other data provider properties as something else
        // OleDbProvider Property
        //


        //public static DALDataOracle DAL = DataOracle;
        public static DALDataOleDb DAL = DataOleDb;
        //public static DALDataMSSQL DAL = DataMSSQL;



        // To use OleDb, name the OleDbProvider property as DAL and 
        //   rename the other data provider properties as something else
        // OleDbProvider Property
        //public static DALDataOleDb DALOleDb
        //{
        //    get { return DataOleDb; }
        //}

        // To use Oracle, name the OracleProvider property as DAL
        //   rename the other data provider properties as something else
        // OracleProvider Property
        //public static DALDataOracle DALOracle
        //{
        //    get { return DataOracle; }
        //}

        // To use MS SQL, name the MSSQLProvider property as DAL
        //   rename the other data provider properties as something else
        // MSSQLProvider Property
        //public static DALDataMSSQL DALMSSQL
        //{
        //    get { return DataMSSQL; }
        //}
    }
}
