namespace PostgreSql.Native
{
    public enum ConnStatusType
    {
        /// <summary>
        /// Connection is ready.
        /// </summary>
        CONNECTION_OK = 0,
        /// <summary>
        /// Connection has failed.
        /// </summary>
        CONNECTION_BAD = 1,
        /// <summary>
        /// Waiting for connection to be made.
        /// </summary>
        CONNECTION_STARTED = 2,
        /// <summary>
        /// Connection OK; waiting to send.
        /// </summary>
        CONNECTION_MADE = 3,
        /// <summary>
        /// Waiting for a response from the server.
        /// </summary>
        CONNECTION_AWAITING_RESPONSE = 4,
        /// <summary>
        /// Received authentication; waiting for backend start-up to finish.
        /// </summary>
        CONNECTION_AUTH_OK = 5,
        /// <summary>
        /// Negotiating environment-driven parameter settings.
        /// </summary>
        CONNECTION_SETENV = 6,
        /// <summary>
        /// Negotiating SSL encryption.
        /// </summary>
        CONNECTION_SSL_STARTUP = 7,
        CONNECTION_NEEDED = 8,
        CONNECTION_CHECK_WRITABLE = 9,
        CONNECTION_CONSUME = 10
    }
}
