using System;
using System.Data;
using FluentMigrator.Builders.Execute;

namespace FluentMigrator.Runner.Processors.SqlServer
{
    public class SqlServerCe4Processor : ProcessorBase
    {
        private readonly DbFactoryBase factory;

        public IDbConnection Connection { get; set; }
        public IDbTransaction Transaction { get; private set; }

        public override string DatabaseType
        {
            get { return "SqlCe4"; }
        }

        public SqlServerCe4Processor(IDbConnection connection, IMigrationGenerator generator, IAnnouncer announcer, IMigrationProcessorOptions options, DbFactoryBase factory)
            : base(generator, announcer, options)
        {
            this.factory = factory;
            Connection = connection;
            connection.Open();
            BeginTransaction();
        }

        public override void Process(PerformDBOperationExpression expression)
        {
            if (expression.Operation == null)
            {
                return;
            }
            if (Connection.State != ConnectionState.Open) Connection.Open();
            Announcer.Say("PerformDBOperationExpression");

            try
            {
                expression.Operation(Connection, Transaction);
            }
            catch (Exception ex)
            {
                Announcer.Error(ex.Message);
                RollbackTransaction();
                throw;
            }

        }

        protected override void Process(string sql)
        {
            Announcer.Sql(sql);

            if (Options.PreviewOnly || string.IsNullOrEmpty(sql))
                return;

            if (Connection.State != ConnectionState.Open)
                Connection.Open();

            using (var command = factory.CreateCommand(sql, Connection, Transaction))
            {
                try
                {
                    command.CommandTimeout = 0; // SQL Server CE does not support non-zero command timeout values!! :/
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Announcer.Error(ex.Message);
                    RollbackTransaction();
                    throw;
                }
            }
        }

        public override void BeginTransaction()
        {
            Announcer.Say("Beginning Transaction");
            Transaction = Connection.BeginTransaction();
        }

        public override void CommitTransaction()
        {
            Announcer.Say("Committing Transaction");
            Transaction.Commit();

            if (Connection.State != ConnectionState.Closed)
            {
                Connection.Close();
            }
        }

        public override void RollbackTransaction()
        {
            Announcer.Say("Rolling back transaction");

            Transaction.Rollback();

            if (Connection.State != ConnectionState.Closed)
            {
                Connection.Close();
            }
        }

        public override void Execute(string template, params object[] args)
        {
            Process(String.Format(template, args));
        }

        public override bool SchemaExists(string schemaName)
        {
            return true;
        }

        public override bool TableExists(string schemaName, string tableName)
        {
            Announcer.Say("TableExists");
            return Exists("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{0}'", FormatSqlEscape(tableName));
        }

        public override bool ColumnExists(string schemaName, string tableName, string columnName)
        {
            Announcer.Say("ColumnExists");
            return Exists("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}' AND COLUMN_NAME = '{1}'", FormatSqlEscape(tableName), FormatSqlEscape(columnName));
        }

        public override bool ConstraintExists(string schemaName, string tableName, string constraintName)
        {
            Announcer.Say("ConstraintExists");
            return Exists("SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME = '{0}' AND CONSTRAINT_NAME = '{1}'", FormatSqlEscape(tableName), FormatSqlEscape(constraintName));
        }

        public override bool IndexExists(string schemaName, string tableName, string indexName)
        {
            Announcer.Say("IndexExists");
            return Exists("SELECT * FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME = '{0}' AND INDEX_NAME = '{1}'",
                          FormatSqlEscape(tableName), FormatSqlEscape(indexName));
        }

        public override bool IndexExists(string schemaName, string tableName, string indexName, string columnName)
        {
            Announcer.Say("IndexExists - columnName");
            return Exists("SELECT * FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME = '{0}' AND INDEX_NAME = '{1}' AND COLUMN_NAME = '{2}'",
                          FormatSqlEscape(tableName), FormatSqlEscape(indexName), FormatSqlEscape(columnName));
        }

        public override DataSet ReadTableData(string schemaName, string tableName)
        {
            return Read("SELECT * FROM [{0}]", tableName);
        }

        public override DataSet Read(string template, params object[] args)
        {
            if (Connection.State != ConnectionState.Open) Connection.Open();

            var ds = new DataSet();
            using (var command = factory.CreateCommand(String.Format(template, args), Connection, Transaction))
            {
                var adapter = factory.CreateDataAdapter(command);
                adapter.Fill(ds);
                return ds;
            }
        }

        public override bool Exists(string template, params object[] args)
        {
            if (Connection.State != ConnectionState.Open)
                Connection.Open();

            using (var command = factory.CreateCommand(String.Format(template, args), Connection, Transaction))
            using (var reader = command.ExecuteReader())
            {
                var exists = reader.Read();
                reader.Close();
                return exists;
            }
        }

        protected string FormatSqlEscape(string sql)
        {
            return sql.Replace("'", "''");
        }
    }
}