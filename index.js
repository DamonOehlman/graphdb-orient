var async = require('async'),
	debug = require('debug')('graphdb-orient'),
	errors = {
		NOT_CONNECTED: 'Unable to perform operation on disconnected db'
	},
	orientdb = require('orientdb'),
	commands = {},
	_ = require('underscore');

/* define base types handler */

exports.defineBaseTypes = function(types) {
	debug('defining core types for orientdb connection');

	types.define('string');
	types.define('uuid').alias('string');
};

/**
## connect(graph, opts, callback)
*/
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
		db = graph._db = new orientdb.GraphDb(opts.db.name, server, opts.db);

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

/**
## close(graph, callback)
*/
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

/* operation definitions */

/**
## activateType(graph, definition, callback)

The activateType operation handler is used to ensure that a type has been
properly defined within orient.  Within orient, there is a notion of classes
that provide some aspect of types.
*/
exports.activateType = function(graph, definition, callback) {
	var db = graph._db,
		className = definition.type;

	// if we don't have a db connection, abort the operation
	if (! db) return callback(errors.NOT_CONNECTED);

	// attempt to create the class
	db.createClass(className, 'V', function(err) {
        // if we encountered an error, let's flag that type as existing
        // so we do not attempt to create it again
        if (err) {
        	definition.active = true;
            return callback();
        }

        // execute the following commands in series
        commands.series([
            'CREATE PROPERTY ' + className + '.id STRING',
            'CREATE INDEX ' + className + '.id UNIQUE'
        ], db, callback);
	});
};

/**
## createNode(data, callback)
*/
exports.createNode = function(graph, data, callback) {
	var db = graph._db;

	// if we don't have a db connection, abort the operation
	if (! db) return callback(errors.NOT_CONNECTED);

	// ensure we have data
	data = data || {};

	// if we don't have a type for the node, it will simply be of type V
	data.type = data.type || 'V';

	// run the insert statement on the graph
	debug('creating a new node in the graph with data: ', data);

	// get the field values, without the type field
	db.createVertex(_.omit(data, 'type'), { 'class': data.type }, callback);
};

/**
## series(commands, targetDb, callback)

This function is used to execute a series of OrientDB commands in series on
the targetdb.

## parallel(commands, targetDb, callback)

As per the series command, except the commands are executed in parallel
*/
['series', 'parallel'].forEach(function(op) {
    commands[op] = function(commands, targetDb, callback) {
        debug('running commands: ', commands);

        // create the bound command calls
        var boundCommands = commands.map(function(command) {
            return targetDb.command.bind(targetDb, command);
        });

        // execute the commands
        async[op].call(null, boundCommands, callback);
    };
});