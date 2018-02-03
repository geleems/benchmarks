// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PostgreSql;

namespace Benchmarks.Data
{
    public class LibpqDb : IDb
    {
        private readonly IRandom _random;
        private readonly Database _database;

        const string readRandomWorldSQL = "SELECT id, randomnumber FROM world WHERE id = $1";
        const string loadFortunes = "SELECT id, message FROM fortune";

        public LibpqDb(IRandom random, Database database)
        {
            _random = random;
            _database = database;
        }

        public Task<World> LoadSingleQueryRow()
        {
            using (var connection = _database.Connect())
            {
                return Task.FromResult(ReadSingleRow(connection));
            }
        }
        
        private World ReadSingleRow(Connection connection)
        {
            connection.Prepare(nameof(readRandomWorldSQL), readRandomWorldSQL, 1);
            connection.ExecPrepared(nameof(readRandomWorldSQL), new string[] { _random.Next(1, 10001).ToString() });

            return new World
            {
                Id = int.Parse(connection.Value(0, 0)),
                RandomNumber = int.Parse(connection.Value(0, 1))
            };
        }

        public Task<World[]> LoadMultipleQueriesRows(int count)
        {
            var result = new World[count];

            using (var connection = _database.Connect())
            {
                for (var i = 0; i < count; i++)
                {
                    result[i] = ReadSingleRow(connection);
                }
            }

            return Task.FromResult(result);
        }

        public Task<World[]> LoadMultipleUpdatesRows(int count)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<Fortune>> LoadFortunesRows()
        {
            var result = new List<Fortune>();

            using (var connection = _database.Connect())
            {
                connection.Prepare(nameof(loadFortunes), loadFortunes);
                connection.ExecPrepared(nameof(loadFortunes));

                for(var i=0; i< connection.Rows; i++)
                {
                    result.Add(new Fortune
                    {
                        Id = int.Parse(connection.Value(i, 0)),
                        Message = connection.Value(i, 1)
                    });
                }
            }

            result.Add(new Fortune { Message = "Additional fortune added at request time." });
            result.Sort();

            return Task.FromResult<IEnumerable<Fortune>>(result);
        }
    }
}
