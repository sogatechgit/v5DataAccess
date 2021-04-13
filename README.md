# C# Server-Side DataAccess Project
## Class DALTable
### Constructor:
```C#
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
            JObject captions = null,
            JArray tableLinks = null,
            JObject tableLinksFields = null,
            Dictionary<string, DALTable> tableCollection=null,
            bool isLinkTable = false,
            bool autoKey = true)

```
### Properties:
```C#
public DateTime instantiated
public string tableName
public string tableFieldPrefix
public string tableClassFilename
public string tableClass
public string tableRowClass
public string tableCode
public JArray links
public JObject captions
public JArray tableLinks
public JObject tableLinksFields

public string description
public List<string> log
public string templateString
public string templateImports

public string templateClass
public bool autoKey
public List<ColumnInfo> columns
public Dictionary<string,ColumnInfo> columnsIndex
public Dictionary<string, DALTable> tableCollection
public List<ColumnInfo> keyCols
public List<ColumnInfo> sortCols
public List<ColumnInfo> groupCols
public List<ColumnInfo> uniqueCols
public Int64 NewAutoId
public ColumnInfo keyCol
public ColumnInfo grpCol
public bool isDataTable
public bool isLinkTable
public string appDataPath
public string appSchemaFile
public string appTemplateFile

// New Properties - 2020-04-21 **********************************************************************************

// property containing all Linked Table definitions 
// (i.e. bridge link (w/ & w/o stored data), parent-child, parent-lookup/parent-lookup(groupped))
public Dictionary<string, DALRelation> tableRelations { set; get; }        

// *public string childSQL


```

### Methods:
```C#
private void Initialize()
private void CollectAllColumns()
// *private void GenerateTypeScriptSchema()
// *private void ExtractActualColumns()
```

### Functions
```C#
public string GetActionFromData(JObject data)
public bool colExist(string name,Dictionary<string,ColumnInfo> index=null)
private string MapDataTypeToTS(string dataType)
private bool isFileExists(string file = "", string path = "")

public ReturnObject Post(JArray values, JObject args = null)
public List<CommandParam> GetCommandParamsForPosting(JArray values,JObject args = null)

public List<ReturnObject> Get(JObject args = null, int objOrder = -1)

public ColumnInfo GetColumnByName(string columnName)
public ColumnInfo GetColumnByBoolParam(string booleanParameter)

//--------------------------------------------------------------------------------------------------------
public string SQLText(
            string mode = "fields",
            int ctr = 0,
            List<ColumnInfo> includeColumns = null,
            List<ColumnInfo> whereColumns = null,
            List<ColumnInfo> sortColumns = null,
            bool noCond = false,
            bool noSort = false,
            bool byGroup = false,
            bool fromInsert = false,
            DALTable parentTable=null,
            string parentField=null)

// - mode
//	"select"
//	"update"
//	"insert"
// --------------------------------------------------------------------------------------------------------


public List<ColumnInfo> ColumnsFromIndices(string colIndices)
public Dictionary<string, dynamic> BuildCommandParamsFromKeys(string keyVals, string keyIndices = "")
public Dictionary<string, dynamic> BuildCommandParams(JObject args, List<ColumnInfo> cols)

// 0-public ReturnObjectExternal SQLTest()
// 0-public string GetSelectSQL()
// 0-private string SQLGet()
// 0-public Dictionary<string,dynamic> BuildUpdateParams(JObject values)
```

### Specical Objects
```C#
AppTables[_g.KEY_TABLE_UPDATE_TRACK_CODE] - Reference to ChangeTracking DALTable object
where _g.KEY_TABLE_UPDATE_TRACK_CODE = "chgTrack"
```

## Class DALTableFieldParams
### Constructor
```C#
public DALTableFieldParams(JObject data, 
            DALStamps stamps,
            DALTable table,
            bool isUpdating = true,
            List<ColumnInfo> keyFields = null)
```
### Properties
```C#
private DALStamps stamps
private bool isUpdating
private DALTable table
public List<ColumnInfo> keyFields
public string SQLText
public List<ColumnInfo> columns
public Dictionary<string, dynamic> parameters
public Int64 tempNewId

```

### Methods

```C#
public void FormatData(JObject data, DALStamps stamps,bool reset = false)
```
## Class DALTableLink
### Contructor:
```C#
public DALTableLink(string code,Dictionary<string,DALTable> tables,JObject args=null)
```

### Properties:
```C#
public string code
public string key
public DALStamps stamps
public DALStamps childStamps
public DALTable table
public string childCode
public string childParentKey
public bool hasChild

```
## Class DALTableUpdateParams
### Contructor
```C#
public DALTableUpdateParams(DALTable table, JObject data, JObject args = null)
```

## Switching Data Provider
```c#
        // DataAccess.DALData

        // Switch Data Provider by assigning the correct Provider class to DAL property

        // To use OleDb, name the OleDbProvider property as DAL and 
        //   rename the other data provider properties as something else
        // OleDbProvider Property
        public static DALDataOleDb DAL
        {
            get { return DataOleDb; }
        }

        // To use Oracle, name the OracleProvider property as DAL
        //   rename the other data provider properties as something else
        // OracleProvider Property
        public static DALDataOracle DALOracle
        {
            get { return DataOracle; }
        }
        
        // To use MS SQL, name the MSSQLProvider property as DAL
        //   rename the other data provider properties as something else
        // MSSQLProvider Property
        public static DALDataMSSQL DALMSSQL
        {
            get { return DataMSSQL; }
        }
        
```

### Recent Updates
#### April 24, 2020
#### April 23, 2020
#### April 22, 2020
- Started witn client-side data formatting for posting
  - routine for selective change detection to minimize transfer size
- implementation data posting feedback routines
#### April 21, 2020
- Continued with linked table update routines
  - link table CRUD operation
  - implement change tracking routine
#### April 20, 2020
- Started working on linked table updates
  - design link table structure
  - set table configuration for linked tables
  - link table CRUD operation
  
  
### To Do's
- [ ] POST Method - Create new record action and feedback
- [ ] DELETE Method - Delete action on records and linked records
- [ ] Handling mandatory fields
- [ ] Handling duplicate entries
- [ ] Client-side data formatting for posting
- [ ] Enhanced user authentication process
- [ ] Auto-generated linked table (optional)

#### Earlier Updates
```text
- ...
- worked on multi-command posting with transaction
- data class abstracting
- implementation of change log and record-locking feature

```
### Updating in progress ...
