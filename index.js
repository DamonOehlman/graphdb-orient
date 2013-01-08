var orientdb = require('orientdb');

exports.connect = function(graph, opts, callback) {
	// ensure we have valid opts
	opts = opts || {};

	// if we don't have server configuration details, trigger a callback
	if (! opts.server) {
		return callback(new Error('server connection details required to use orientdb connector'));
	}

	if (! opts.db) {
		return callback(new Error('db name, username and password require to use orientdb connector'));
	}

	// define the core orientdb types
	graph.types.define('string');

	// create the graph connection
	graph._server = new orientdb.Server(opts.server);

	// create the db instance
	graph._db = new orientdb.Db(opts.db.name, graph._server, opts.db);
};