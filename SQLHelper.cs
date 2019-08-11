using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Collections;
using System.Diagnostics;
using System.Data.Common;
using System.Collections.Generic;
using System.Text;
using DBHelper.BaseHelper;
using System.Linq;
using CommonLib;
using CommLiby;
using DBHelper.Common;

namespace SQLHelper
{
    public class SQLHelper : DBHelper.DBHelper
    {
        public SQLHelper(string connectionString) : base(connectionString)
        {
        }

        public override int ExecuteNonQuery(CommandInfo cmdInfo)
        {
            //Create a connection
            using (SqlConnection connection = new SqlConnection())
            {
                try
                {
                    connection.ConnectionString = ConnectionString;
                    // Create a new Sql command
                    SqlCommand cmd = new SqlCommand();
                    //Prepare the command
                    PrepareCommand(cmd, connection, cmdInfo.Text, null, cmdInfo.Type, cmdInfo.Parameters);

                    //Execute the command
                    int val = cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    return val;
                }
                catch (Exception ex)
                {
                    if (Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "SqlHelper.ExecuteNonQuery");
                    return -1;
                }
            }
        }

        public override int ExecuteProcedure(string storedProcName, params DbParameter[] parameters)
        {
            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionString;
                SqlCommand cmd = new SqlCommand();
                try
                {
                    PrepareCommand(cmd, conn, storedProcName, null, CommandType.StoredProcedure, parameters);
                    int i = cmd.ExecuteNonQuery();
                    return i;
                }
                catch (Exception ex)
                {
                    if (Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "SqlHelper.ExecuteProcedure");
                    return -1;
                }
                finally
                {
                    cmd.Dispose();
                }
            }
        }

        public override int ExecuteProcedureTran(string storedProcName, params DbParameter[] parameters)
        {
            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                SqlTransaction tran = conn.BeginTransaction();
                SqlCommand cmd = new SqlCommand();
                try
                {
                    PrepareCommand(cmd, conn, storedProcName, tran, CommandType.StoredProcedure, parameters);
                    int i = cmd.ExecuteNonQuery();
                    tran.Commit();
                    return i;
                }
                catch (Exception ex)
                {
                    tran.Rollback();
                    if (Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "SqlHelper.ExecuteProcedureTran");
                    return -1;
                }
                finally
                {
                    tran.Dispose();
                    cmd.Dispose();
                }
            }
        }

        public override DbDataReader ExecuteReader(string sqlString, params DbParameter[] dbParameter)
        {
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = ConnectionString;
            SqlCommand cmd = new SqlCommand();
            SqlDataReader rdr = null;
            try
            {
                //Prepare the command to execute
                PrepareCommand(cmd, conn, sqlString, null, CommandType.Text, dbParameter);
                rdr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                return rdr;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex);
                rdr?.Close();
                cmd.Dispose();
                conn.Close();
            }
            return null;
        }

        public override int ExecuteSqlsTran(List<CommandInfo> cmdList, int num = 5000)
        {
            if (cmdList == null || cmdList.Count == 0) return -1;

            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                int allCount = 0;

                //Stopwatch watch = new Stopwatch();
                while (cmdList.Count > 0)
                {
                    //watch.Reset();
                    //watch.Start();                
                    var submitSQLs = cmdList.Take(num);
                    SqlTransaction tx = conn.BeginTransaction();
                    SqlCommand cmd = new SqlCommand();
                    int count = 0;
                    try
                    {
                        foreach (CommandInfo c in submitSQLs)
                        {
                            try
                            {
                                if (!string.IsNullOrEmpty(c.Text))
                                {
                                    PrepareCommand(cmd, conn, c.Text, tx, c.Type, c.Parameters);
                                    int res = cmd.ExecuteNonQuery();
                                    if (c.EffentNextType == EffentNextType.ExcuteEffectRows && res == 0)
                                    {
                                        throw new Exception("Sql:违背要求" + c.Text + "必须有影响行");
                                    }
                                    count += res;
                                }
                            }
                            catch (Exception ex)
                            {
                                if (c.FailRollback)
                                    throw ex;
                            }
                        }
                        tx.Commit();
                    }
                    catch (Exception ex)
                    {
                        tx.Rollback();
                        if (Debugger.IsAttached)
                            throw new Exception(ex.Message);
                        else
                            LogHelper.Error(ex, "SqlHelper.ExecuteSqlsTran");
                        count = 0;
                        break;
                    }
                    finally
                    {
                        cmd.Dispose();
                        tx.Dispose();
                        allCount += count;
                    }

                    int removeCount = cmdList.Count >= num ? num : cmdList.Count; //每次最多执行1000行
                    cmdList.RemoveRange(0, removeCount);
                    //watch.Stop();
                    //Console.WriteLine(cmdList.Count + "-" + allCount + "-" + watch.ElapsedMilliseconds / 1000);
                }
                return allCount;
            }
        }

        public override int ExecuteSqlTran(CommandInfo cmdInfo)
        {
            //Create a connection
            using (SqlConnection connection = new SqlConnection())
            {
                connection.ConnectionString = ConnectionString;
                connection.Open();
                SqlTransaction tran = connection.BeginTransaction();
                try
                {
                    // Create a new Sql command
                    SqlCommand cmd = new SqlCommand();
                    //Prepare the command
                    PrepareCommand(cmd, connection, cmdInfo.Text, tran, cmdInfo.Type, cmdInfo.Parameters);

                    //Execute the command
                    int val = cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    return val;
                }
                catch (Exception ex)
                {
                    tran.Rollback();
                    if (Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "SqlHelper.ExecuteSqlTran");
                    return -1;
                }
                finally
                {
                    tran.Dispose();
                }
            }
        }

        public override DataBaseType GetCurrentDataBaseType()
        {
            return DataBaseType.SqlServer;
        }

        public override string GetPageRowNumSql(string dataSql, int startRowNum, int endRowNum)
        {
            return $"{dataSql} offset {startRowNum} row fetch next {endRowNum - startRowNum} row only";
        }

        public override string GetRowLimitSql(string dataSql, int rowLimit)
        {
            return $"select top {rowLimit} z_.* from ({dataSql}) z_";
        }

        public override DataSet Query(string sqlString, params DbParameter[] dbParameter)
        {
            DataSet ds = new DataSet("ds");
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                try
                {
                    using (SqlCommand cmd = new SqlCommand())
                    {
                        PrepareCommand(cmd, connection, sqlString, null, CommandType.Text, dbParameter);
                        using (SqlDataAdapter command = new SqlDataAdapter())
                        {
                            command.SelectCommand = cmd;
                            command.Fill(ds, "dt");
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (System.Diagnostics.Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "SqlHelper.Query");
                }
            }
            return ds;
        }

        public override object QueryScalar(string sqlString, CommandType cmdType = CommandType.Text, params DbParameter[] dbParameter)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                using (SqlCommand cmd = new SqlCommand())
                {
                    PrepareCommand(cmd, connection, sqlString, null, cmdType, dbParameter);
                    object obj = cmd.ExecuteScalar();
                    if ((Equals(obj, null)) || (Equals(obj, DBNull.Value)))
                    {
                        return null;
                    }
                    else
                    {
                        return obj;
                    }
                }
            }
        }

        public override bool TableExists(string tableName)
        {
            throw new NotImplementedException();
        }

        public override bool TestConnectionString()
        {
            DataTable dt = QueryTable("select 1");
            if (dt.IsNotEmpty())
            {
                if (dt.Rows[0][0].ToString() == "1")
                {
                    return true;
                }
            }
            return false;
        }
    }
}
