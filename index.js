var async = require('async'),
	debug = require('debug')('graphdb-orient'),
	orientdb = require('orientdb');

exports.defineBaseTypes = function(types) {
	debug('defining core types for orientdb connection');

	types.define('string');
	types.define('uuid').alias('string');
};

exports.connect = function(graph, opts, callback) {
	var db, server;

	// ensure we have valid opts
	opts = opts || {};

	// if we don't have server configuration details, trigger a callback
	if (! opts.server) {
		return callback(new Error('server connection details required to use orientdb connector'));
	}

	if (! opts.db) {
		return callback(new Error('db name, username and password require to use orientdb connector'));
	}

	// create the graph connection
	server = graph._server = new orientdb.Server(opts.server);

	// connect the server
	server.connect(function(err) {
		// create the db instance
		db = graph._db = new orientdb.Db(opts.db.name, server, opts.db);

		// attempt to open the database
		// and if that fails, attempt to create the db and then open it
	    debug('attempting to open the ' + opts.db.name + ' db');
	    db.open(function(err) {
	        // if we had no error, then fire the callback and return
	        if (! err) return callback();

	        // otherwise, we need to attempt to create the db and then open it
	        debug('unable to open db, attempting to create new db');
	        async.series([
	            db.create.bind(db),
	            db.open.bind(db)
	        ], callback);
	    });	
	});
};

exports.close = function(graph, callback) {
	// if we have no db, then return the callback
	if (! graph._db) return callback();

	// close the database, and once done clear the _db member
	graph._db.close(function() {
		graph._db = undefined;

		// pass the callback through
		callback.apply(this, arguments);
	});
};