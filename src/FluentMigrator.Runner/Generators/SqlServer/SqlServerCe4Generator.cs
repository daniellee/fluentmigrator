using System;
using FluentMigrator.Expressions;

namespace FluentMigrator.Runner.Generators.SqlServer
{
    public class SqlServerCe4Generator : SqlServer2000Generator
    {
        public override string Generate(RenameTableExpression expression)
        {
            // Sql Ce4 does not like square brackets in the with sp_rename
            return String.Format("sp_rename '{0}', '{1}'", FormatSqlEscape(expression.OldName), FormatSqlEscape(expression.NewName));
        }

        private static string FormatSqlEscape(string sql)
        {
            return sql.Replace("'", "''");
        }
    }
}