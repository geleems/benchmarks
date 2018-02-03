using System;
using System.Threading;
using static PostgreSql.Native.Libpq;

namespace PostgreSql
{
    /// <summary>
    /// A <see cref="Database"/> instance represents a PostgreSql server configuration.
    /// Public members of the class are thread-safe. An instance of this class should be shared across the application for a single connection string.
    /// Each <see cref="Database"/> instance contains a pool of connections.
    public class Database : IDisposable
    {
        /// TODO: A timer could clean the pool after some time when connections are not used

        public const UInt16 DefaultMaxPoolSize = 128;
        public const UInt16 DefaultGrowthFactor = 10;
        public const UInt16 DefaultMinPoolSize = 2;
        
        internal Connection[] _connections;

        /// <summary>
        /// Creates a new <see cref="Database"/> instance from a connection string.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public Database(string connectionString) : this(connectionString, DefaultMaxPoolSize, DefaultMinPoolSize, DefaultGrowthFactor)
        {
        }

        /// <summary>
        /// Creates a new <see cref="Database"/> instance from a connection string and a connection pool size.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="maxPoolSize">The maximum size of the pool.</param>
        /// <param name="minPoolSize">The minimum size of the pool.</param>
        /// <param name="growthFactor">The growth factor of the pool in connections.</param>
        public Database(string connectionString, UInt16 maxPoolSize = DefaultMaxPoolSize, UInt16 minPoolSize = DefaultMinPoolSize, UInt16 growthFactor = DefaultGrowthFactor)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            // Use the connection string in a supported libpq format
            if (connectionString.StartsWith("postgresql://"))
            {
                ConnectionString = connectionString;
            }
            else
            {
                // Try to extract the parameters from an ADO.NET connection string
                ConnectionString = "";

                var segments = connectionString.Split(";");
                foreach (var segment in segments)
                {
                    var values = segment.Split("=");

                    switch (values[0].ToLowerInvariant())
                    {
                        case "server":
                            ConnectionString += "host=" + values[1] + " ";
                            break;
                        case "database":
                            ConnectionString += "dbname=" + values[1] + " ";
                            break;
                        case "user id":
                            ConnectionString += "user=" + values[1] + " ";
                            break;
                        case "password":
                            ConnectionString += "password=" + values[1] + " ";
                            break;
                    }
                }
            }

            MaxPoolSize = maxPoolSize;
            MinPoolSize = minPoolSize;
            GrowthFactor = growthFactor;

            _connections = new Connection[MinPoolSize];
        }

        /// <summary>
        /// Returns the growth factor of the pool in connections.
        /// </summary>
        public UInt16 GrowthFactor { get; private set; }

        /// <summary>
        /// Returns the minimum size of the pool.
        /// </summary>
        public UInt16 MinPoolSize { get; private set; }

        /// <summary>
        /// Returns the maximum size of the pool.
        /// </summary>
        public UInt16 MaxPoolSize { get; private set; }

        /// <summary>
        /// Returns the connection string used to create new connections
        /// </summary>
        public string ConnectionString { get; private set; }

        public void Dispose()
        {
            foreach (var connection in _connections)
            {
                if (connection != null)
                {
                    connection.Dispose(false);
                    GC.SuppressFinalize(connection);
                }
            }
        }

        /// <summary>
        /// Returns a connection to the database.
        /// </summary>
        /// <remarks>
        /// If new instance is only created if none is available in the connection pool.</remarks>
        /// <returns>A <see cref="Connection"/> instance or <c>null</c> if the connection failed.</returns>
        public Connection Connect()
        {
            for (var i = 0; i < _connections.Length; i++)
            {
                var item = _connections[i];
                if (item != null && Interlocked.CompareExchange(ref _connections[i], null, item) == item)
                {
                    // Only return the connection if it's valid
                    if (item.ConnStatus == Native.ConnStatusType.CONNECTION_OK)
                    {
                        return item;
                    }
                    else
                    {
                        // Dispose the connection without returning it to the pool
                        item.Dispose(false);
                        GC.SuppressFinalize(item);
                    }
                }
            }

            return CreateConnection();
        }

        private Connection CreateConnection()
        {
            // Perf: parse the connection string and call PQsetdbLogin which should be faster
            var pgConn = PQconnectdb(ConnectionString);

            if (pgConn == null)
            {
                return null;
            }

            if (PQstatus(pgConn) == Native.ConnStatusType.CONNECTION_BAD)
            {
                return null;
            }

            var connection = new Connection(pgConn, this);

            return connection;
        }

        internal bool Return(Connection connection)
        {
            // Only accept valid connections in the pool
            if (PQstatus(connection._pgConn) != Native.ConnStatusType.CONNECTION_OK)
            {
                return false;
            }

            while(true)
            {
                for (var i = 0; i < _connections.Length; i++)
                {
                    if (Interlocked.CompareExchange(ref _connections[i], connection, null) == null)
                    {
                        return true;
                    }
                }

                // There is no more space on the pool, do we need to grow it?
                if (_connections.Length < MaxPoolSize)
                {
                    var newPoolSize = Math.Min(_connections.Length + GrowthFactor, MaxPoolSize);

                    Array.Resize(ref _connections, newPoolSize);
                }
                else
                {
                    return false;
                }
            }            
        }
    }
}
