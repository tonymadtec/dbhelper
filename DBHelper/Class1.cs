using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Data;

namespace DBHelperNameSpace
{
    public class DBHelper
    {
        #region 属性

        /// <summary>
        /// 链接字符串
        /// </summary>
        private string conStr;

        /// <summary>
        /// DB工厂
        /// </summary>
        private DbProviderFactory provider;

        #endregion

        #region 构造函数

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="key">链接字符串键</param>
        public DBHelper(string key)
        {
            if (string.IsNullOrEmpty(key))
            {
                throw new ArgumentNullException("key");
            }
            ConnectionStringSettings css = System.Web.Configuration.WebConfigurationManager.ConnectionStrings[key];
            if (css == null)
            {
                throw new InvalidOperationException("未找到指定的链接字符串！");
            }
            this.conStr = css.ConnectionString;
            this.provider = DbProviderFactories.GetFactory(css.ProviderName);
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="conStr">链接字符串</param>
        /// <param name="providerStr">数据源提供程序</param>
        public DBHelper(string conStr, string providerStr)
        {
            if (string.IsNullOrEmpty(conStr))
            {
                throw new ArgumentNullException("conStr");
            }
            if (string.IsNullOrEmpty(providerStr))
            {
                throw new ArgumentNullException("providerStr");
            }
            this.provider = DbProviderFactories.GetFactory(providerStr);
            this.conStr = conStr;
        }

        #endregion

        #region 外部方法

        /// <summary>
        /// 执行命令，返回受影响行数
        /// </summary>
        /// <param name="commandType">命令类型</param>
        /// <param name="sql">sql语句或存储过程名称</param>
        /// <param name="parameters">参数数组</param>
        /// <returns>受影响行数,失败返回-1</returns>
        public virtual int ExecuteNonQuery(CommandType commandType, string sqlOrProcName, IEnumerable<DbParameter> parameters)
        {
            DbConnection con = CreateConnection();
            DbCommand cmd = CreateCommand(con, commandType, sqlOrProcName, parameters);
            try
            {
                con.Open();
                return cmd.ExecuteNonQuery();
            }
            catch
            {
                throw;
            }
            finally
            {
                cmd.Dispose();
                con.Close();
            }
        }

        /// <summary>
        /// 执行命令，返回第一行第一列对象
        /// </summary>
        /// <param name="commandType">命令类型</param>
        /// <param name="sql">sql语句或存储过程名称</param>
        /// <param name="parameters">参数数组</param>
        /// <returns>执行结果</returns>
        public virtual object ExecuteScalar(CommandType commandType, string sqlOrProcName, IEnumerable<DbParameter> parameters)
        {
            DbConnection con = CreateConnection();
            DbCommand cmd = CreateCommand(con, commandType, sqlOrProcName, parameters);
            try
            {
                con.Open();
                return cmd.ExecuteScalar();
            }
            catch
            {
                throw;
            }
            finally
            {
                cmd.Dispose();
                con.Close();
            }
        }

        /// <summary>
        /// 执行命令返回DataSet
        /// </summary>
        /// <param name="commandType">命令类型</param>
        /// <param name="sql">sql语句或存储过程名称</param>
        /// <param name="parameters">参数数组</param>
        /// <returns>DataSet</returns>
        public virtual DataSet GetDataSet(CommandType commandType, string sqlOrProcName, IEnumerable<DbParameter> parameters)
        {
            DbConnection con = CreateConnection();
            DbCommand cmd = CreateCommand(con, commandType, sqlOrProcName, parameters);
            DataSet set = new DataSet();
            DbDataAdapter adapter = this.provider.CreateDataAdapter();
            try
            {
                con.Open();
                adapter.SelectCommand = cmd;
                adapter.Fill(set);
                return set;
            }
            catch
            {
                throw;
            }
            finally
            {
                adapter.Dispose();
                cmd.Dispose();
                con.Close();
            }
        }

        /// <summary>
        /// 执行命令返回DbDataReader
        /// </summary>
        /// <param name="commandType">命令类型</param>
        /// <param name="sql">sql语句或存储过程名称</param>
        /// <param name="parameters">参数数组</param>
        /// <param name="action">委托</param>
        /// <returns>对象列表</returns>
        public virtual List<T> ExecuteReader<T>(CommandType commandType, string sqlOrProcName, IEnumerable<DbParameter> parameters,
            Func<DbDataReader, T> action)
        {
            DbConnection con = CreateConnection();
            DbCommand cmd = CreateCommand(con, commandType, sqlOrProcName, parameters);
            List<T> result = new List<T>();
            try
            {
                con.Open();
                DbDataReader reader = cmd.ExecuteReader();
                try
                {
                    while (reader.Read())
                    {
                        var item = action(reader);
                        result.Add(item);
                    }
                    return result;
                }
                catch
                {
                    throw;
                }
                finally
                {
                    reader.Dispose();
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                cmd.Dispose();
                con.Close();
            }
        }

        /// <summary>
        /// 批量执行sql语句
        /// </summary>
        /// <param name="sqlList">sql语句集合</param>
        /// <param name="paramList">参数数组集合</param>
        /// <returns>执行成功或失败</returns>
        public virtual bool ExecuteSqlBatchByTrans(IEnumerable<string> sqlList, IEnumerable<List<DbParameter>> paramList)
        {
            DbConnection con = CreateConnection();
            DbCommand cmd = CreateCommand(con, CommandType.Text);
            try
            {
                con.Open();
                DbTransaction trans = con.BeginTransaction();
                cmd.Transaction = trans;
                try
                {
                    int length = sqlList.Count();
                    IEnumerable<DbParameter> parameters = null;
                    for (int i = 0; i < length; i++)
                    {
                        cmd.CommandText = sqlList.ElementAt<string>(i);
                        cmd.Parameters.Clear();
                        parameters = paramList.ElementAt<List<DbParameter>>(i);
                        foreach (DbParameter pm in parameters)
                        {
                            cmd.Parameters.Add(pm);
                        }
                        cmd.ExecuteNonQuery();
                    }
                    trans.Commit();
                    return true;
                }
                catch
                {
                    trans.Rollback();
                    throw;
                }
                finally
                {
                    trans.Dispose();
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                cmd.Dispose();
                con.Close();
            }
        }

        #endregion

        #region CreateDbParameter

        public DbParameter CreateDbParameter(string name, object value)
        {
            DbParameter parameter = this.provider.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            return parameter;
        }

        public DbParameter CreateDbParameter(string name, object value, ParameterDirection direction)
        {
            DbParameter parameter = this.provider.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            parameter.Direction = direction;
            return parameter;
        }

        public DbParameter CreateDbParameter(string name, object value, int size)
        {
            DbParameter parameter = this.provider.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            parameter.Size = size;
            return parameter;
        }

        public DbParameter CreateDbParameter(string name, object value, int size, DbType type)
        {
            DbParameter parameter = this.provider.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            parameter.Size = size;
            parameter.DbType = type;
            return parameter;
        }

        public DbParameter CreateDbParameter(string name, object value, int size, DbType type, ParameterDirection direction)
        {
            DbParameter parameter = this.provider.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            parameter.Size = size;
            parameter.DbType = type;
            parameter.Direction = direction;
            return parameter;
        }

        #endregion

        #region 私有方法

        /// <summary>
        /// 获取链接实例
        /// </summary>
        /// <returns>链接实例</returns>
        private DbConnection CreateConnection()
        {
            DbConnection con = this.provider.CreateConnection();
            con.ConnectionString = this.conStr;
            return con;
        }

        /// <summary>
        /// 获取命令实例
        /// </summary>
        /// <param name="con">链接实例</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="sqlOrProcName">sql语句或存储过程名称</param>
        /// <returns>命令实例</returns>
        private DbCommand CreateCommand(DbConnection con, CommandType commandType, string sqlOrProcName, IEnumerable<DbParameter> parameters)
        {
            DbCommand cmd = InitCommand(con, commandType, parameters);
            cmd.CommandText = sqlOrProcName;
            return cmd;
        }

        /// <summary>
        /// 获取命令实例
        /// </summary>
        /// <param name="con">链接实例</param>
        /// <param name="commandType">命令类型</param>
        /// <returns>命令实例</returns>
        private DbCommand CreateCommand(DbConnection con, CommandType commandType)
        {
            return InitCommand(con, commandType, null);
        }

        /// <summary>
        /// 初始化命令
        /// </summary>
        /// <param name="commandType">命令类型</param>
        /// <param name="parameters">参数集合</param>
        /// <returns></returns>
        private DbCommand InitCommand(DbConnection con, CommandType commandType, IEnumerable<DbParameter> parameters)
        {
            DbCommand cmd = con.CreateCommand();
            cmd.CommandType = commandType;
            if (parameters != null)
            {
                foreach (DbParameter pm in parameters)
                {
                    cmd.Parameters.Add(pm);
                }
            }
            return cmd;
        }

        #endregion
    }
}
