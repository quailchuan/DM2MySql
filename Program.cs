  internal class Program
  {
      static List<DbTableInfo> Tables = null;
      static List<DbColumnInfo> Columns = null;
      static Dictionary<string, string[]> PrimaryKey = null;
      static int index = 0;
      static void Main(string[] args)
      {

          List<DBServer> dbs = new List<DBServer>()
          {
             new DBServer()
             {
                  ConfigId=1,
                   Database="DAMENG",
                    DbType= SqlSugar.DbType.Dm,
                     Host="0.0.0.0",
                     Port=5236,
                      Password="SYSDBA",
                      UserName="SYSDBA",
                       Schema="NATIONAL_DEBT"
             },
              new DBServer()
             {
                  ConfigId=2,
                   Database="National_Debt",
                    DbType= SqlSugar.DbType.MySql,
                     Host="0.0.0.0",
                     Port=3306,
                      UserName="test",
                      Password="test",
                       Schema=""
              }
          };


          Tables = GetTable(dbs[0]);
          Columns = GetCoulums(dbs[0]);
          GetPrimaryKey(dbs[0]);
          CreateTables();
          CopyData();
      }


      ///<summary>
      /// 获取表
      ///</summary>
      static List<DbTableInfo> GetTable(DBServer dB)
      {
          List<DbTableInfo> tabInfo = new List<DbTableInfo>();
          if(dB.DbType == SqlSugar.DbType.Dm)
          {
              string sql = @$"
                select T.TABLE_NAME,C.COMMENTS from 
                ALL_TABLES T left join ALL_TAB_COMMENTS C
                on T.TABLE_NAME=C.TABLE_NAME
                WHERE T.OWNER='{dB.Schema}' and C.OWNER='{dB.Schema}'
              ";
              DataTable dt = SqlSugarHelper.Db.GetConnection(dB.ConfigId).Ado.GetDataTable(sql);
              foreach(DataRow item in dt.Rows)
              {
                  tabInfo.Add(new DbTableInfo { Description = item[1].ToString(), Name = item[0].ToString() });
              }
              return tabInfo;
          }
          else
          {
              return null;
          }


      }

      ///<summary>
      /// 获取字段
      ///</summary>
      static List<DbColumnInfo> GetCoulums(DBServer dB)
      {
          List<DbColumnInfo> colunmInfo = new List<DbColumnInfo>();
          if(dB.DbType == SqlSugar.DbType.Dm)
          {
              string sql = @$"
                              select COL.COLUMN_NAME, 
                                     COL.DATA_TYPE, 
                                     COL.DATA_LENGTH, 
                                     COL.DATA_PRECISION, 
                                     COL.DATA_SCALE, 
                                     COL.NULLABLE, 
                                     COM.COMMENTS,
                                     COL.TABLE_NAME 
                                  from ALL_TAB_COLUMNS COL 
                          LEFT JOIN ALL_COL_COMMENTS COM
                                  ON COL.TABLE_NAME=COM.TABLE_NAME 
                                  AND COL.COLUMN_NAME=COM.COLUMN_NAME
                              WHERE COL.OWNER='{dB.Schema}' 
                                  AND COM.SCHEMA_NAME='{dB.Schema}'
                                  
          ";
              DataTable dt = SqlSugarHelper.Db.GetConnection(dB.ConfigId).Ado.GetDataTable(sql);
              int decimalDigits = 0, scale = 0;
              foreach(DataRow item in dt.Rows)
              {
                  decimalDigits = string.IsNullOrWhiteSpace(item[3].ToString()) ? 0 : int.Parse(item[3].ToString());
                  scale = string.IsNullOrWhiteSpace(item[4].ToString()) ? 0 : int.Parse(item[4].ToString());

                  colunmInfo.Add(new DbColumnInfo
                  {

                      DbColumnName = item[0].ToString(),
                      DataType = item[1].ToString(),
                      Length = int.Parse(item[2].ToString() ?? "0"),
                      DecimalDigits = decimalDigits,
                      Scale = scale,
                      IsNullable = item[5].ToString() == "Y",
                      ColumnDescription = item[6].ToString(),
                      TableName = item[7].ToString(),
                  });
              }
              return colunmInfo;
          }
          else
          {
              return null;
          }
      }
      ///<summary>
      /// 获取主键
      ///</summary>
      static void GetPrimaryKey(DBServer dB)
      {

          if(dB.DbType == SqlSugar.DbType.Dm)
          {
              string sql = $@"
                      SELECT CC.TABLE_NAME, 
                            CC.COLUMN_NAME 
                       FROM ALL_CONS_COLUMNS CC
                  LEFT JOIN ALL_CONSTRAINTS C 
                         ON CC.CONSTRAINT_NAME=C.CONSTRAINT_NAME
                      WHERE CC.OWNER='{dB.Schema}' 
                        AND C.CONSTRAINT_TYPE='P'
              ";

              DataTable dt = SqlSugarHelper.Db.GetConnection(dB.ConfigId).Ado.GetDataTable(sql);

              PrimaryKey = new Dictionary<string, string[]>();


              foreach(DataRow item in dt.Rows)
              {

                  if(PrimaryKey.ContainsKey(item[0].ToString()))
                  {
                      string[] data = PrimaryKey[item[0].ToString()];
                      var list = data.ToList();
                          list.Add(item[1].ToString());
                      PrimaryKey[item[0].ToString()] = list.ToArray();
                  }
                  else PrimaryKey.Add(item[0].ToString(), new string[] { item[1].ToString() });



              }


          }

      }
      ///<summary>
      /// 创建表
      ///</summary>
      static bool CreateTables()
      {
          StringBuilder sql = new StringBuilder();
          foreach(DbTableInfo item in Tables)
          {
              sql.AppendLine($@"create table {item.Name}(");

              var list = Columns.Where(x => x.TableName.Equals(item.Name)).ToList();

              for(int i = 0; i < list.Count; i++)
              {
                  var col = list[i];


                  sql.Append($@"`{col.DbColumnName}`");
                  if(col.DataType.ToLower() == "varchar") sql.Append($@"varchar({col.Length}) ");
                  else if(col.DataType.ToUpper() == "NUMBER") sql.Append($@"bit(1) ");
                  else if(col.DataType.ToUpper() == "TIMESTAMP") sql.Append($@"datetime ");
                  else if(col.DataType.ToUpper() == "DECIMAL") sql.Append($@"DECIMAL({col.DecimalDigits},{col.Scale}) ");
                  else if(col.DataType.ToUpper() == "LONGVARCHAR") sql.Append($@"longtext ");
                  else sql.Append($@"{col.DataType} ");
                  if(col.IsNullable) sql.Append($@"Null ");
                  else sql.Append($@"Not Null ");
                  sql.Append($@"comment '{col.ColumnDescription}',");
                  sql.Append(Environment.NewLine);
              }

              if(PrimaryKey.ContainsKey(item.Name))
              {

                  sql.AppendLine($@"primary key({string.Join(',', PrimaryKey[item.Name])})");
              }
              else
              {
                  sql.Remove(sql.Length - 3, 3);
                  sql.Append(Environment.NewLine);
              }
            
              sql.AppendLine($@") comment '{item.Description}';");
          }

          Console.WriteLine(sql.ToString());
          try
          {
              SqlSugarHelper.Db.GetConnection(2).Ado.BeginTran();
              SqlSugarHelper.Db.GetConnection(2).Ado.ExecuteCommand(sql.ToString());
              SqlSugarHelper.Db.GetConnection(2).Ado.CommitTran();
          }
          catch(Exception ex)
          {
              SqlSugarHelper.Db.GetConnection(2).Ado.RollbackTran();
              return false;
          }
          
          return true;
      }
      ///<summary>
      /// 传输数据
      ///<summary>
      static bool CopyData()
      {

          try
          {
              SqlSugarHelper.Db.GetConnection(2).Ado.BeginTran();
              foreach(var tab in Tables)
              {
                  var list=Columns.Where(x => x.TableName.Equals(tab.Name)).ToList();
                  string[] cols = Columns.Where(x => x.TableName.Equals(tab.Name)).Select(x => x.DbColumnName).ToArray();
                  string temp = string.Empty;
                  foreach(string col in cols) {
                      temp += "\"" + col + "\",";
                  }
                 temp= temp.Remove(temp.Length - 1);
                  string qsql = $@"select {temp} from  NATIONAL_DEBT.""{tab.Name}""";
                  DataTable dt = null;
                  dt=SqlSugarHelper.Db.GetConnection(1).Ado.GetDataTable(qsql);
                  index++;
                  SqlSugarHelper.Db.GetConnection(2).Fastest<System.Data.DataTable>().AS(tab.Name).BulkCopy(dt);
              }

               SqlSugarHelper.Db.GetConnection(2).Ado.CommitTran();
              return true;
          }
          catch(Exception ex)
          {

              SqlSugarHelper.Db.GetConnection(2).Ado.RollbackTran();
              return false;
          }
      }

  }
