var async = require('async'),
	debug = require('debug')('graphdb-orient'),
	errors = {
		NOT_CONNECTED: 'Unable to perform operation on disconnected db'
	},
	orientdb = require('orientdb'),
	orientParser = require('orientdb/lib/orientdb/connection/parser'),
	commands = {},
	_ = require('underscore'),
    templates = {
        update: 'UPDATE <%= type %> <%= sqlsets %> WHERE id = "<%= id %>"',
        vertexCreate: 'CREATE VERTEX <%= type %> <%= sqlsets %>'
    };

// compile each of the templates
_.each(templates, function(value, key) {
    templates[key] = _.template(value);
});

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
## createEdge(graph, sourceId, targetId, data, callback)
*/
exports.createEdge = function(graph, sourceId, targetId, data, callback) {

};

/**
## getNode(graph, id, nodeType, callback)
*/
exports.getNode = function(graph, id, nodeType, callback) {
	var db = graph._db;

	// if we don't have a db connection, abort the operation
	if (! db) return callback(errors.NOT_CONNECTED);

	// look for the node details
    db.command(
      'SELECT FROM ' + (nodeType || 'V') + ' WHERE id = "' + id + '"',
      function(err, results) {
      	callback(err, (results || [])[0]);
      }
    );
};

/**
## saveNode(graph, node, callback)
*/
exports.saveNode = function(graph, node, callback) {
	var db = graph._db;

	// if we don't have a db connection, abort the operation
	if (! db) return callback(new Error(errors.NOT_CONNECTED));

	// if we don't have node data, then report an invalid node
	if (! node.data) return callback(new Error('A node object is require for a save operation'));

	// look for an existing node
	exports.getNode(graph, node.data.id, node.type, function(err, existing) {
		var commandTemplate = templates[existing ? 'update' : 'vertexCreate'],
            data = _.omit(node.data, existing ? ['id'] : []),
            commandText = commandTemplate({
                type: node.type,
                sqlsets: orientParser.hashToSQLSets(data).sqlsets,
                id: node.data.id
            });

		if (err) return callback(err);

		// run the command
		debug('running command: ' + commandText);
		db.command(commandText, callback);
	});
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