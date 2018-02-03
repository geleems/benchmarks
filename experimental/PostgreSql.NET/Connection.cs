using System;
using System.Collections.Generic;
using PostgreSql.Native;
using static PostgreSql.Native.Libpq;

namespace PostgreSql
{
    public class Connection : IDisposable
    {
        internal readonly PgConn _pgConn;
        internal readonly Database _factory;
        internal PgResult _lastResult;
        internal Dictionary<string, string> _preparedQueries;
        
        internal Connection(PgConn pgConn, Database factory)
        {
            _pgConn = pgConn;
            _factory = factory;
        }

        public PgResult LastResult
        {
            get { return _lastResult; }
            private set { ClearLastResult(); _lastResult = value; }
        }

        private void ClearLastResult()
        {
            if (_lastResult == null)
            {
                return;
            }

            PQclear(_lastResult);
            _lastResult = null;
        }

        /// <summary>
        /// Submits a command to the server and waits for the result.
        /// </summary>
        /// <param name="commandText">The command text.</param>
        public void Exec(string commandText)
        {
            LastResult = PQexec(_pgConn, commandText);
        }

        /// <summary>
        /// Submits a command to the server and waits for the result, with the ability to pass parameters separately from the SQL command text.
        /// </summary>
        /// <param name="commandText">The command text.</param>
        /// <param name="commandParams">The command parameters.</param>
        public unsafe void ExecParams(string commandText, string[] commandParams)
        {
            // Because all parameters are of type string, we can omit the parameter information.
            // If we wanted to use other types (int, bool, ...) we would need to provide them.

            var buffer = commandParams.ToByteArray();
            fixed (byte* bufferPtr = buffer)
            {
                LastResult = PQexecParams(_pgConn, commandText, commandParams.Length, null, &bufferPtr, null, null, 0);
            }
        }

        /// <summary>
        /// Submits a request to create a prepared statement with the given parameters, and waits for completion.
        /// </summary>
        /// <param name="statementName">The name of the statement.</param>
        /// <param name="commandText">The statement.</param>
        /// <param name="parameters">The number of parameters in the statements.</param>
        /// <remarks>
        /// Prepared statements are cached at the connection level. If a named statement is already prepared
        /// then the value of <see cref="LastResult"/> will be null as no request will be sent to the database
        /// server.
        /// </remarks>
        public void Prepare(string statementName, string commandText, int parameters = 0)
        {
            if (_preparedQueries == null)
            {
                _preparedQueries = new Dictionary<string, string>();
            }

            if (_preparedQueries.TryGetValue(statementName, out var preparedCommandText) && commandText == preparedCommandText)
            {
                // Query already prepared for this connection
                return;
            }
            else
            {
                _preparedQueries[statementName] = commandText;
            }

            LastResult = PQprepare(_pgConn, statementName, commandText, parameters, null);
        }

        /// <summary>
        /// Sends a request to execute a prepared statement with given parameters, and waits for the result.
        /// </summary>
        /// <param name="statementName">The name of the statement.</param>
        /// <param name="commandParameters">The command parameters.</param>
        public unsafe void ExecPrepared(string statementName, string[] commandParameters = null)
        {
            var parameters = commandParameters ?? Array.Empty<string>();

            var buffer = parameters.ToByteArray();
            fixed (byte* bufferPtr = buffer)
            {
                LastResult = PQexecPrepared(_pgConn, statementName, parameters.Length, &bufferPtr, null, null, 0);
            }
        }

        /// <summary>
        /// Returns the number of rows in the query result.
        /// </summary>
        public int Rows => PQntuples(LastResult);

        /// <summary>
        /// Returns the number of fields in each row of the query result.
        /// </summary>
        public int Fields => PQnfields(LastResult);

        /// <summary>
        /// Returns the name of a field from its index in the result.
        /// </summary>
        /// <param name="field">The index of the field in the result.</param>
        /// <returns>The name of the field.</returns>
        public string Name(int field) => PQfname(LastResult, field);

        /// <summary>
        /// Returns the field index in the result from its name.
        /// </summary>
        /// <param name="fieldName">The name of the field.</param>
        /// <returns>The index of the field or -1 if it doesn't exist.</returns>
        public int Index(string fieldName)
        {
            return PQfnumber(LastResult, fieldName);
        }

        /// <summary>
        /// Returns a single field value of one row from the previous command execution.
        /// </summary>
        /// <param name="row">The index of the row.</param>
        /// <param name="field">The index of the field</param>
        /// <returns>The value of the field.</returns>
        public string Value(int row, int field)
        {
            return PQgetvalue(LastResult, row, field);
        }

        /// <summary>
        /// Returns a single field value of one row from the previous command execution.
        /// </summary>
        /// <param name="row">The index of the row.</param>
        /// <param name="name">The name of the field</param>
        /// <returns>The value of the field.</returns>
        public string Value(int row, string name)
        {
            return Value(row, Index(name));
        }

        /// <summary>
        /// Tests a field for a null value. Row and field numbers start at 0.
        /// </summary>
        /// <param name="row">The index of the row.</param>
        /// <param name="field">The index of the field</param>
        /// <returns><c>True</c> if the value is null.</returns>
        public bool IsNull(int row, int field)
        {
            return PQgetisnull(LastResult, row, field) == 1;
        }

        /// <summary>
        /// Tests a field for a null value. Row and field numbers start at 0.
        /// </summary>
        /// <param name="row">The index of the row.</param>
        /// <param name="fieldName">The name of the field</param>
        /// <returns><c>True</c> if the value is null.</returns>
        public bool IsNull(int row, string fieldName)
        {
            return IsNull(row, Index(fieldName));
        }

        /// <summary>
        /// Returns the result status of the last executed command.
        /// </summary>
        public ExecStatusType ExecStatus => PQresultStatus(LastResult);

        /// <summary>
        /// Returns the connection status.
        /// </summary>
        public ConnStatusType ConnStatus => PQstatus(_pgConn);
        public string ErrorMessage => PQresultErrorMessage(LastResult);

        /// <summary>
        /// Frees the associated resources and tries to add it back to the connection pool.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        internal void Dispose(bool reuseable)
        {
            if (reuseable)
            {
                // Get rid of managed resources
            }

            ClearLastResult();

            // Prepared queries are reused while the connection is living

            // If the finalizer was called or we couldn't return it to the pool, release the unmanaged resources
            if (!reuseable || !_factory.Return(this))
            {
                // Get rid of unmanaged resources
                PQfinish(_pgConn);
            }
        }

        ~Connection()
        {
            Dispose(false);
        }
    }
}
