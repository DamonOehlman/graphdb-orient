var async = require('async'),
    debug = require('debug')('graphdb-orient'),
    orientDebug = require('debug')('orientdb'),
    errors = {
        NOT_CONNECTED: 'Unable to perform operation on disconnected db'
    },
    orienteer = require('orienteer'),
    commands = {},
    _ = require('underscore'),
    templates = {
        selectById: 'SELECT FROM <%= type %> WHERE id = "<%= id %>"',
        selectEdge: 'SELECT FROM <%= type %> ' +
            'WHERE in.id = "<%= source.id %>" ' +
            'AND out.id = "<%= target.id %>"',

        update: 'UPDATE <%= type %> <%= sqlsets %> WHERE id = "<%= id %>"',
        vertexCreate: 'CREATE VERTEX <%= type %> <%= sqlsets %>',
        edgeCreate: 'CREATE EDGE <%= type %> FROM ' +
            '(SELECT FROM <%= source.type %> WHERE id = "<%= source.id %>") TO ' +
            '(SELECT FROM <%= target.type %> WHERE id = "<%= target.id %>") ' +
            '<%= sqlsets %>'
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
    types.define('integer');
    types.define('float');

    // define some of the orient list and set types
    types.define('list').alias('embeddedlist');
    types.define('set').alias('embeddedset');
};

/**
## connect(graph, opts, callback)
*/
exports.connect = function(graph, opts, callback) {
    var connection;

    // ensure we have valid opts
    opts = opts || {};

    // wrap the callback to create a "debug" back
    callback = debuggable(callback);

    // if we don't have server configuration details, trigger a callback
    if (! opts.protocol) {
        return callback(new Error('server connection protocol required to use orientdb connector'));
    }

    if (! opts.db) {
        return callback(new Error('db name, username and password require to use orientdb connector'));
    }

    // initialise the connection
    connection = orienteer(opts);

    // check that the required database exists
    connection.dbExist({ name: opts.db }, function(err, result) {
        if (err || (! result) || (! result.exists)) {
            return new callback(err || new Error('Could not find db: ' + opts.db));
        }

        // update the connection to use the admin user by default
        graph._connection = connection = connection.as('admin');

        // otherwise, use the db and trigger the callback
        connection.db(opts.db);
        callback();
    });
};

/**
## close(graph, callback)
*/
exports.close = function(graph, callback) {
    // if we have no db, then return the callback
    if (! graph._connection) return callback();

    // TODO: close the connection
    graph._connection = undefined;
    callback();
};

/* operation definitions */

/**
## activateNodeType(graph, definition, callback)

The active node type is used to ensure the specified type definition exists
within OrientDB and inherits from the `OGraphVertex` class (which is aliased to `V`).
*/
exports.activateNodeType = function(graph, definition, callback) {
    activateType(graph, definition, 'OGraphVertex', callback);
};

/**
## activateEdgeType(graph, definition, callback)

The activateEdgeType function is used to ensure the type has exists in OrientDB.
*/
exports.activateEdgeType = function(graph, definition, callback) {
    activateType(graph, definition, 'OGraphEdge', callback);
};

/**
## find(graph, searchParams, opts, callback)
*/
exports.find = function(graph, searchParams, opts, callback) {
    // if an id has been requested and a type specified, then do a get by id
    if (searchParams.id && searchParams.type) {
        debug('searching by id, using type: ' + searchParams.type);
        return findById(graph, searchParams.id, searchParams.type, callback);
    }
    // otherwise, if we have an id, but no type look for a graphvertex
    // and then a graph edge if no vertex is found
    else if (searchParams.id) {
        // first look for a vertex
        debug('searching by id but no type, looking for a vertex first');
        findById(graph, searchParams.id, 'OGraphVertex', function(err, results) {
            if (err || results.length > 0) return callback(err, results);

            // no hit on the vertex, so look for an edge
            debug('could not locate vertex, looking for an edge');
            return findById(graph, searchParams.id, 'OGraphEdge', callback);
        });
    }
    else {
        return callback(null, []);
    }
};

exports.getEdge = function(graph, source, target, edgeType, callback) {
    var command = templates.selectEdge({
            type: edgeType || 'OGraphEdge',
            source: source,
            target: target
        });

    // if we don't have a db connection, abort the operation
    if (! graph._connection) return callback(errors.NOT_CONNECTED);

    // look for the node details
    sendCommand(graph._connection, command, function(err, results) {
        callback(err, (results || [])[0]);
    });
};

/**
## saveNode(graph, node, callback)
*/
exports.saveNode = function(graph, node, callback) {
    // if we don't have a db connection, abort the operation
    if (! graph._connection) return callback(new Error(errors.NOT_CONNECTED));

    // if we don't have node data, then report an invalid node
    if (! node.data) return callback(new Error('A node object is require for a save operation'));

    // if the node type is not defined, then create it
    node.type = node.type || 'OGraphVertex';

    // look for an existing node
    findById(graph, node.data.id, node.type, function(err, results) {
        var existing = (! err) && results.length > 0,
            commandTemplate = templates[existing ? 'update' : 'vertexCreate'],
            data = _.omit(node.data, existing ? ['id'] : []),
                commandText = commandTemplate({
                    type: node.type,
                sqlsets: orienteer.objectTo('SET', data),
                id: existing && results[0].id
            });

        if (err) return callback(err);

        // run the command
        sendCommand(graph._connection, commandText, callback);
    });
};

exports.saveEdge = function(graph, source, target, entity, callback) {
    // if we don't have a db connection, abort the operation
    if (! graph._connection) return callback(new Error(errors.NOT_CONNECTED));

    // if we don't have node data, then report an invalid node
    if (! entity.data) return callback(new Error('A valid entity is require for a save operation'));

    // look for an existing node
    exports.getEdge(graph, source, target, entity.type, function(err, existing) {
        var commandTemplate = templates[existing ? 'update' : 'edgeCreate'],
            data = _.omit(entity.data, existing ? ['id'] : []),
            commandText = commandTemplate({
                type: entity.type || 'OGraphEdge',
                sqlsets: orienteer.objectTo('SET', data),
                source: source,
                target: target,
                id: existing && existing.id
            });

        if (err) return callback(err);

        // run the command
        sendCommand(graph._connection, commandText, callback);
    });
};

/* internal helper functions */

/**
## activateType(graph, definition, callback)

The activateType operation handler is used to ensure that a type has been
properly defined within orient.  Within orient, there is a notion of classes
that provide some aspect of types.
*/
function activateType(graph, definition, baseClass, callback) {
    var className = definition.type;

    // if we don't have a db connection, abort the operation
    if (! graph._connection) return callback(errors.NOT_CONNECTED);

    // create the class and required properties
    commands.series([
        'CREATE CLASS ' + className + ' EXTENDS ' + baseClass,
        'CREATE PROPERTY ' + className + '.id STRING',
        'CREATE INDEX ' + className + '.id UNIQUE'
    ], graph._connection, callback);
}

/**
## debuggable(callback)

This is simple function that provides debug info on error conditions
*/
function debuggable(callback) {
    return function(err) {
        if (err instanceof Error) {
            debug('error: ' + err.message);
        }

        callback.apply(this, arguments);
    };
}

/**
## findById(graph, id, className, callback)

The findById function is used internally to find a specific object by id.
*/
function findById(graph, id, className, callback) {
    var command = templates.selectById({ type: className, id: id });

    // if we don't have a db connection, abort the operation
    if (! graph._connection) return callback(errors.NOT_CONNECTED);

    // look for the node details
    sendCommand(graph._connection, command, callback);
}

/**
## sendCommand(db, command, callback)

The sendCommand function is used internally to provide debug tracking on the commands
send to orientdb
*/
function sendCommand(connection, command, callback) {
    orientDebug(command);
    connection.sql(command, function(err, results) {
        if (err) orientDebug('error: ', err);
        callback.apply(this, arguments);
    });
}

/**
## series(commands, targetDb, callback)

This function is used to execute a series of OrientDB commands in series on
the targetdb.

## parallel(commands, targetDb, callback)

As per the series command, except the commands are executed in parallel
*/
['series', 'parallel'].forEach(function(op) {
    commands[op] = function(commands, connection, callback) {
        // create the bound command calls
        var boundCommands = commands.map(function(command) {
            return function(commandCallback) {
                orientDebug(command);
                connection.sql(command, commandCallback);
            };
        });

        // execute the commands
        async[op].call(null, boundCommands, callback);
    };
});